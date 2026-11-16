package example.scalaspark

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, ExprId, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.SparkSession

import scala.jdk.CollectionConverters._

final case class ColumnLineage(output: String, inputs: Seq[String])

object Lineage {
  private val mapper = new ObjectMapper()

  def session(app: String, lineageDir: String): SparkSession = {
    val spark = Workload.session(app)
    Files.createDirectories(Paths.get(lineageDir))
    spark.listenerManager.register(new FileLineageListener(lineageDir, app))
    spark
  }

  def writeSummary(dir: String, extra: Map[String, Any] = Map.empty): Path = {
    val path = Paths.get(dir, "summary.json")
    val events = readEvents(dir)
    val datasets = events.flatMap { e =>
      val ins = asMaps(e.get("inputs"))
      val outs = asMaps(e.get("outputs"))
      (ins ++ outs).map { d =>
        Map(
          "name" -> String.valueOf(d.getOrDefault("name", "")),
          "namespace" -> String.valueOf(d.getOrDefault("namespace", "file")),
          "role" -> (if (outs.contains(d) || outs.exists(_.get("name") == d.get("name"))) "output" else "input")
        )
      }
    }.distinct
    val columns = events.flatMap(e => asMaps(e.get("columnLineage"))).map { c =>
      Map(
        "output" -> String.valueOf(c.getOrDefault("output", "")),
        "inputs" -> c.getOrDefault("inputs", java.util.List.of())
      )
    }
    val doc = Map[String, Any](
      "event_count" -> events.size,
      "datasets" -> datasets,
      "columnLineage" -> columns
    ) ++ extra
    writeJson(path, doc)
    path
  }

  def readEvents(dir: String): Seq[java.util.Map[String, Any]] = {
    val file = Paths.get(dir, "events.jsonl")
    if (!Files.exists(file)) Seq.empty
    else
      Files
        .readAllLines(file, StandardCharsets.UTF_8)
        .asScala
        .map(_.trim)
        .filter(_.nonEmpty)
        .map(line => mapper.readValue(line, classOf[java.util.Map[String, Any]]))
        .toSeq
  }

  def extract(qe: QueryExecution, funcName: String, durationMs: Long, jobName: String): Map[String, Any] = {
    val plan = qe.analyzed
    val outputs = writeTargets(plan)
    val inputs = readSources(plan)
    val query = writeQuery(plan).getOrElse(plan)
    val columns = columnLineage(query, inputs)
    Map(
      "eventType" -> "COMPLETE",
      "eventTime" -> now(),
      "producer" -> "example.scalaspark.FileLineageListener",
      "schemaURL" -> "https://openlineage.io/spec/1-0-5/OpenLineage.json",
      "job" -> Map("namespace" -> "spark-daria-medallion", "name" -> jobName, "funcName" -> funcName),
      "run" -> Map("durationMs" -> durationMs),
      "inputs" -> inputs.map(namedDataset),
      "outputs" -> outputs.map(namedDataset),
      "columnLineage" -> columns.map(c => Map("output" -> c.output, "inputs" -> c.inputs)),
      "logicalPlan" -> plan.treeString
    )
  }

  def isWrite(funcName: String, qe: QueryExecution): Boolean = {
    val name = funcName.toLowerCase
    name.contains("save") || name.contains("insert") || name.contains("command") ||
    writeTargets(qe.analyzed).nonEmpty
  }

  private def writeTargets(plan: LogicalPlan): Seq[String] =
    plan.collect {
      case c: InsertIntoHadoopFsRelationCommand => c.outputPath.toString
    }.distinct

  private def writeQuery(plan: LogicalPlan): Option[LogicalPlan] =
    plan.collectFirst { case c: InsertIntoHadoopFsRelationCommand => c.query }

  private def readSources(plan: LogicalPlan): Seq[String] =
    plan.collect {
      case LogicalRelation(rel: HadoopFsRelation, _, _, _, _) =>
        rel.location.rootPaths.map(_.toString)
    }.flatten.distinct

  private def columnLineage(plan: LogicalPlan, inputPaths: Seq[String]): Seq[ColumnLineage] = {
    val leaves: Map[ExprId, String] = plan
      .collect {
        case l: LogicalRelation =>
          val src = location(l).getOrElse(inputPaths.headOption.getOrElse("input"))
          l.output.map(a => a.exprId -> s"$src#${a.name}")
      }
      .flatten
      .toMap
    val outputs = plan match {
      case p: Project => p.projectList
      case other      => other.output
    }
    outputs.map { e =>
      val name = e match {
        case a: Attribute => a.name
        case a: Alias     => a.name
        case other        => other.prettyName
      }
      val inputs = origins(e, leaves).distinct.sorted
      ColumnLineage(name, inputs)
    }
  }

  private def origins(e: Expression, leaves: Map[ExprId, String]): Seq[String] = e match {
    case a: Attribute            => leaves.get(a.exprId).toSeq :+ a.name
    case Alias(child, _)         => origins(child, leaves)
    case other if other.children.isEmpty =>
      other.references.toSeq.flatMap(r => origins(r, leaves))
    case other => other.children.flatMap(c => origins(c, leaves))
  }

  private def location(rel: LogicalRelation): Option[String] = rel.relation match {
    case h: HadoopFsRelation => h.location.rootPaths.headOption.map(_.toString)
    case _                   => None
  }

  private def namedDataset(path: String): Map[String, String] =
    Map("namespace" -> "file", "name" -> path, "type" -> "parquet")

  private def asMaps(v: Any): Seq[java.util.Map[String, Any]] = v match {
    case xs: java.util.List[_] =>
      xs.asScala.collect { case m: java.util.Map[_, _] => m.asInstanceOf[java.util.Map[String, Any]] }.toSeq
    case _ => Seq.empty
  }

  private def now(): String = {
    val fmt = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ")
    java.time.ZonedDateTime.now(java.time.ZoneId.of("Asia/Tbilisi")).format(fmt)
  }

  def writeJson(path: Path, value: Any): Unit = {
    Files.createDirectories(path.getParent)
    Files.write(path, pretty(value).getBytes(StandardCharsets.UTF_8))
  }

  def appendJsonl(path: Path, value: Any): Unit = synchronized {
    Files.createDirectories(path.getParent)
    Files.write(
      path,
      (compact(value) + "\n").getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE,
      StandardOpenOption.APPEND
    )
  }

  def javaize(v: Any): Any = v match {
    case m: Map[_, _] =>
      m.iterator
        .map { case (k, x) => String.valueOf(k) -> javaize(x) }
        .toMap
        .asJava
    case s: Seq[_] => s.map(javaize).asJava
    case Some(x)   => javaize(x)
    case None      => null
    case other     => other
  }

  private def pretty(v: Any): String = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(javaize(v))
  private def compact(v: Any): String = mapper.writeValueAsString(javaize(v))
}

class FileLineageListener(outDir: String, jobName: String) extends QueryExecutionListener {
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
    if (Lineage.isWrite(funcName, qe)) {
      val event = Lineage.extract(qe, funcName, durationNs / 1000000, jobName)
      Lineage.appendJsonl(Paths.get(outDir, "events.jsonl"), event)
    }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    val event = Map(
      "eventType" -> "FAIL",
      "job" -> Map("name" -> jobName, "funcName" -> funcName),
      "error" -> Option(exception).map(_.getMessage).getOrElse("unknown")
    )
    Lineage.appendJsonl(Paths.get(outDir, "events.jsonl"), event)
  }
}
