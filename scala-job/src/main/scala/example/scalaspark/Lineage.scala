package example.scalaspark

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, ExprId, Expression, GetStructField}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.jdk.CollectionConverters._

final case class FieldRef(dataset: String, field: String) {
  def qualified: String = if (dataset.nonEmpty) s"$dataset:$field" else field
}

final case class ColumnLineage(outputDataset: String, output: String, inputs: Seq[FieldRef])

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
    val datasets = events
      .flatMap { e =>
        val ins = asMaps(e.get("inputs"))
        val outs = asMaps(e.get("outputs"))
        (ins ++ outs).map { d =>
          val raw = String.valueOf(d.getOrDefault("name", ""))
          Map(
            "name" -> raw,
            "short" -> shortName(raw),
            "namespace" -> String.valueOf(d.getOrDefault("namespace", "file")),
            "role" -> (if (outs.exists(_.get("name") == d.get("name"))) "output" else "input")
          )
        }
      }
      .distinct
    val edges = events
      .flatMap { e =>
        val ins = asMaps(e.get("inputs")).map(d => shortName(String.valueOf(d.getOrDefault("name", ""))))
        val outs = asMaps(e.get("outputs")).map(d => shortName(String.valueOf(d.getOrDefault("name", ""))))
        for (i <- ins; o <- outs if i.nonEmpty && o.nonEmpty && i != o) yield Map("from" -> i, "to" -> o)
      }
      .distinct
    val rawColumns = events.flatMap { e =>
      val outs = asMaps(e.get("outputs")).map(d => shortName(String.valueOf(d.getOrDefault("name", ""))))
      val fallbackDs = outs.headOption.getOrElse("")
      asMaps(e.get("columnLineage")).flatMap { c =>
        val output = String.valueOf(c.getOrDefault("output", ""))
        val outputDs = Option(c.get("outputDataset"))
          .map(String.valueOf)
          .map(_.trim)
          .filter(_.nonEmpty)
          .getOrElse(fallbackDs)
        val inputs = parseInputs(c.get("inputs"))
        if (output.isEmpty) None else Some((outputDs, output, inputs))
      }
    }
    val adj = mutable.Map[(String, String), Seq[FieldRef]]()
    rawColumns.foreach { case (ds, field, inputs) =>
      val key = (ds, field)
      adj(key) = (adj.getOrElse(key, Seq.empty) ++ inputs).distinct
    }
    val columns = rawColumns
      .map { case (ds, field, _) => composedColumn(ds, field, adj.toMap) }
      .filterNot(isIdentity)
      .distinct
    val doc = Map[String, Any](
      "event_count" -> events.size,
      "datasets" -> datasets,
      "edges" -> edges,
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
    val outputDataset = outputs.headOption.map(shortName).getOrElse("")
    val columns = columnLineage(query, inputs, outputDataset)
    Map(
      "eventType" -> "COMPLETE",
      "eventTime" -> now(),
      "producer" -> "example.scalaspark.FileLineageListener",
      "schemaURL" -> "https://openlineage.io/spec/1-0-5/OpenLineage.json",
      "job" -> Map("namespace" -> "spark-daria-medallion", "name" -> jobName, "funcName" -> funcName),
      "run" -> Map("durationMs" -> durationMs),
      "inputs" -> inputs.map(namedDataset),
      "outputs" -> outputs.map(namedDataset),
      "columnLineage" -> columns.map { c =>
        Map(
          "outputDataset" -> c.outputDataset,
          "output" -> c.output,
          "inputs" -> c.inputs.map(r => Map("dataset" -> r.dataset, "field" -> r.field))
        )
      },
      "logicalPlan" -> plan.treeString
    )
  }

  def isWrite(funcName: String, qe: QueryExecution): Boolean = {
    val name = funcName.toLowerCase
    name.contains("save") || name.contains("insert") || name.contains("command") ||
    writeTargets(qe.analyzed).nonEmpty
  }

  def shortName(path: String): String = {
    val cleaned = path.replaceFirst("^file:", "").stripSuffix("/")
    val parts = cleaned.split("/").filter(_.nonEmpty)
    if (parts.isEmpty) cleaned
    else {
      val last = parts.last
      val parent = if (parts.length >= 2) parts(parts.length - 2) else ""
      if (parent == "gold") s"gold/$last" else last
    }
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

  private def columnLineage(
      plan: LogicalPlan,
      inputPaths: Seq[String],
      outputDataset: String
  ): Seq[ColumnLineage] = {
    val sources = mutable.Map[ExprId, FieldRef]()
    val aliases = mutable.Map[ExprId, Expression]()
    plan.foreach { node =>
      node match {
        case l: LogicalRelation =>
          val src = shortName(location(l).orElse(inputPaths.headOption).getOrElse("input"))
          l.output.foreach(a => sources(a.exprId) = FieldRef(src, a.name))
        case _ =>
      }
      node.expressions.foreach(collectAliases(_, aliases))
    }
    val sourceMap = sources.toMap
    val aliasMap = aliases.toMap
    plan.output.flatMap { attr =>
      val inputs = resolve(attr, sourceMap, aliasMap, Set.empty).distinct.sortBy(_.qualified)
      if (inputs.isEmpty) None
      else Some(ColumnLineage(outputDataset, attr.name, inputs))
    }
  }

  private def collectAliases(e: Expression, aliases: mutable.Map[ExprId, Expression]): Unit = {
    e match {
      case a: Alias =>
        aliases(a.exprId) = a.child
        collectAliases(a.child, aliases)
      case other => other.children.foreach(collectAliases(_, aliases))
    }
  }

  private def resolve(
      e: Expression,
      sources: Map[ExprId, FieldRef],
      aliases: Map[ExprId, Expression],
      seen: Set[ExprId]
  ): Seq[FieldRef] = e match {
    case a: Attribute =>
      if (seen.contains(a.exprId)) Seq.empty
      else
        sources.get(a.exprId) match {
          case Some(src) => Seq(src)
          case None =>
            aliases.get(a.exprId) match {
              case Some(child) => resolve(child, sources, aliases, seen + a.exprId)
              case None        => Seq.empty
            }
        }
    case a: Alias =>
      if (seen.contains(a.exprId)) resolve(a.child, sources, aliases, seen)
      else
        sources.get(a.exprId) match {
          case Some(src) => Seq(src)
          case None      => resolve(a.child, sources, aliases, seen + a.exprId)
        }
    case g: GetStructField =>
      val field = structFieldName(g)
      resolve(g.child, sources, aliases, seen).map(parent => parent.copy(field = appendField(parent.field, field)))
    case other =>
      other.children.flatMap(c => resolve(c, sources, aliases, seen)).distinct
  }

  private def structFieldName(g: GetStructField): String =
    g.name.getOrElse(g.childSchema.fields(g.ordinal).name)

  private def appendField(parent: String, field: String): String =
    if (parent == field || parent.endsWith("." + field)) parent else s"$parent.$field"

  private def location(rel: LogicalRelation): Option[String] = rel.relation match {
    case h: HadoopFsRelation => h.location.rootPaths.headOption.map(_.toString)
    case _                   => None
  }

  private def parseInputs(v: Any): Seq[FieldRef] = v match {
    case xs: java.util.List[_] =>
      xs.asScala.toSeq.flatMap {
        case m: java.util.Map[_, _] =>
          val dataset = String.valueOf(Option(m.get("dataset")).getOrElse(""))
          val field = String.valueOf(Option(m.get("field")).orElse(Option(m.get("name"))).getOrElse(""))
          if (field.nonEmpty && field != "null") Some(FieldRef(dataset, field)) else None
        case s: CharSequence => parseRef(s.toString)
        case _               => None
      }
    case s: CharSequence => parseRef(s.toString).toSeq
    case _               => Seq.empty
  }

  private def parseRef(raw: String): Option[FieldRef] = {
    val text = raw.trim
    if (text.isEmpty) None
    else if (text.contains(":")) {
      val i = text.indexOf(':')
      Some(FieldRef(text.substring(0, i), text.substring(i + 1)))
    } else if (text.contains("#")) {
      val i = text.indexOf('#')
      Some(FieldRef(shortName(text.substring(0, i)), text.substring(i + 1)))
    } else Some(FieldRef("", text))
  }

  private def composedColumn(
      dataset: String,
      field: String,
      adj: Map[(String, String), Seq[FieldRef]]
  ): Map[String, Any] = {
    val paths = expandPaths(dataset, field, adj, Set.empty).map(collapse).filter(_.nonEmpty).distinct
    val sources = leafRefs(dataset, field, adj, Set.empty).distinct.sortBy(_.qualified)
    Map(
      "outputDataset" -> dataset,
      "output" -> field,
      "inputs" -> sources.map(r => Map("dataset" -> r.dataset, "field" -> r.field, "qualified" -> r.qualified)),
      "path" -> paths.map(_.mkString(" → ")).mkString("; ")
    )
  }

  private def expandPaths(
      dataset: String,
      field: String,
      adj: Map[(String, String), Seq[FieldRef]],
      seen: Set[(String, String)]
  ): Seq[Seq[String]] = {
    val key = (dataset, field)
    if (seen.contains(key)) Seq(Seq(field))
    else {
      val ins = adj.getOrElse(key, Seq.empty).filter(r => r.field.nonEmpty)
      if (ins.isEmpty) Seq(Seq(field))
      else
        ins.flatMap { in =>
          val parentDs = if (in.dataset.nonEmpty) in.dataset else dataset
          expandPaths(parentDs, in.field, adj, seen + key).map(_ :+ field)
        }
    }
  }

  private def leafRefs(
      dataset: String,
      field: String,
      adj: Map[(String, String), Seq[FieldRef]],
      seen: Set[(String, String)]
  ): Seq[FieldRef] = {
    val key = (dataset, field)
    if (seen.contains(key)) Seq.empty
    else {
      val ins = adj.getOrElse(key, Seq.empty).filter(_.field.nonEmpty)
      if (ins.isEmpty) Seq(FieldRef(dataset, field))
      else
        ins.flatMap { in =>
          val parentDs = if (in.dataset.nonEmpty) in.dataset else dataset
          val next = leafRefs(parentDs, in.field, adj, seen + key)
          if (next.isEmpty) Seq(in) else next
        }
    }
  }

  private def collapse(path: Seq[String]): Seq[String] =
    path.foldLeft(Vector.empty[String]) { (acc, item) =>
      if (acc.lastOption.contains(item)) acc else acc :+ item
    }

  private def isIdentity(row: Map[String, Any]): Boolean = {
    val output = String.valueOf(row.getOrElse("output", ""))
    val path = String.valueOf(row.getOrElse("path", ""))
    val collapsed = path.split("; ").flatMap(_.split(" → ", -1).map(_.trim)).filter(_.nonEmpty).distinct
    output.nonEmpty && (path.isBlank || path == output || collapsed.toSeq == Seq(output))
  }

  private def namedDataset(path: String): Map[String, String] =
    Map("namespace" -> "file", "name" -> path, "type" -> "parquet", "short" -> shortName(path))

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
