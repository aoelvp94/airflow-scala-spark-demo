package example.scalaspark

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import scala.collection.mutable.ArrayBuffer
import scala.util.Using

object PipelineBench {
  def main(args: Array[String]): Unit = {
    System.setProperty("log4j.configurationFile", "log4j2-local.xml")
    val runs = sys.env.get("BENCH_RUNS").map(_.toInt).getOrElse(7)
    val warmup = sys.env.get("BENCH_WARMUP").map(_.toInt).getOrElse(1)
    val uniqueRows = sys.env.get("PIPELINE_ROWS").map(_.toLong).getOrElse(200000L)
    val out = Paths.get(args.headOption.getOrElse("bench/pipeline-local.json")).toAbsolutePath
    val bronzePath = args.lift(1).getOrElse("/tmp/pipeline/bench/bronze")
    val modes = Seq("daria", "plain")

    val t0 = System.nanoTime()
    val spark = Workload.session("pipeline-bench")
    val sparkStartMs = (System.nanoTime() - t0) / 1000000
    println(s"spark_session_start_ms=$sparkStartMs unique_rows=$uniqueRows warmup=$warmup runs=$runs")

    val tBronze0 = System.nanoTime()
    val bronze = Pipeline.bronzeFrame(spark, uniqueRows)
    bronze.write.mode("overwrite").parquet(bronzePath)
    val bronzeRows = spark.read.schema(Pipeline.BronzeSchema).parquet(bronzePath).count()
    val bronzeMs = (System.nanoTime() - tBronze0) / 1000000
    println(s"bronze_rows=$bronzeRows bronze_write_ms=$bronzeMs path=$bronzePath")

    val warm = ArrayBuffer.empty[Map[String, Any]]
    val measured = ArrayBuffer.empty[Map[String, Any]]
    try {
      for (i <- 0 until (warmup + runs)) {
        val tag = if (i < warmup) "warmup" else "bench"
        for (mode <- modes) {
          val rec = one(spark, mode, s"$tag-$i", bronzePath)
          (if (i < warmup) warm else measured) += rec
          println(toJson(rec))
        }
      }
    } finally spark.stop()

    val checksums = measured.map(_("checksum")).toSet
    val rowCounts = measured.map(_("silver_rows")).toSet
    require(checksums.size == 1, s"checksum mismatch: $checksums")
    require(rowCounts.size == 1, s"silver row mismatch: $rowCounts")

    val summary = modes.map { mode =>
      val rs = measured.filter(_("mode") == mode)
      val compute = rs.map(_("compute_ms").asInstanceOf[Long])
      mode -> Map(
        "n" -> rs.size,
        "compute_ms" -> stats(compute),
        "bronze_rows" -> rs.head("bronze_rows"),
        "silver_rows" -> rs.head("silver_rows"),
        "checksum" -> rs.head("checksum"),
        "jvm_hwm_kb" -> stats(rs.map(_("jvm_hwm_kb").asInstanceOf[Long]))
      )
    }.toMap

    val doc = Map[String, Any](
      "generated_at" -> now(),
      "note" -> "Local Spark transform bench on a shared bronze snapshot. Ingest (HTTP) is measured in the Airflow Dag, not here.",
      "spark_daria" -> "1.2.4",
      "spark" -> "4.3.0 local[*]",
      "java" -> System.getProperty("java.version"),
      "unique_rows" -> uniqueRows,
      "bronze_rows" -> bronzeRows,
      "bronze_write_ms" -> bronzeMs,
      "bronze_path" -> bronzePath,
      "runs_per_mode" -> runs,
      "warmup_per_mode" -> warmup,
      "spark_session_start_ms" -> sparkStartMs,
      "host" -> Map(
        "nproc" -> Runtime.getRuntime.availableProcessors(),
        "loadavg_end" -> loadavg()
      ),
      "summary" -> summary,
      "runs" -> measured.toSeq,
      "warmup" -> warm.toSeq
    )
    Files.createDirectories(out.getParent)
    Files.write(out, pretty(doc).getBytes(StandardCharsets.UTF_8))
    println(s"wrote $out")
    println(pretty(summary))
  }

  private def one(
      spark: org.apache.spark.sql.SparkSession,
      mode: String,
      runId: String,
      bronzePath: String
  ): Map[String, Any] = {
    val silverPath = s"/tmp/pipeline/bench/silver-$mode"
    val t1 = System.nanoTime()
    val r = Pipeline.transform(spark, bronzePath, silverPath, mode)
    val t2 = System.nanoTime()
    Map(
      "mode" -> mode,
      "run_id" -> runId,
      "state" -> "success",
      "bronze_rows" -> r.bronzeRows,
      "silver_rows" -> r.silverRows,
      "checksum" -> r.checksum,
      "compute_ms" -> ((t2 - t1) / 1000000),
      "jvm_hwm_kb" -> Workload.vmHwmKb()
    )
  }

  private def stats(vals: Iterable[Long]): Map[String, Long] = {
    val s = vals.toSeq.sorted
    val mid = if (s.size % 2 == 1) s(s.size / 2) else (s(s.size / 2 - 1) + s(s.size / 2)) / 2
    Map("median" -> mid, "min" -> s.head, "max" -> s.last)
  }

  private def now(): String = {
    val fmt = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ")
    java.time.ZonedDateTime.now(java.time.ZoneId.of("Asia/Tbilisi")).format(fmt)
  }

  private def loadavg(): Seq[Double] =
    Using(scala.io.Source.fromFile("/proc/loadavg"))(
      _.mkString.split("\\s+").take(3).map(_.toDouble).toSeq
    ).getOrElse(Seq.empty)

  private def render(v: Any): String = v match {
    case m: Map[_, _] =>
      m.asInstanceOf[Map[String, Any]].toSeq
        .map { case (k, x) => s""""$k": ${render(x)}""" }
        .mkString("{", ", ", "}")
    case s: Iterable[_] if !s.isInstanceOf[Map[_, _]] =>
      s.map(render).mkString("[", ", ", "]")
    case s: String => "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
    case null      => "null"
    case other     => String.valueOf(other)
  }

  private def toJson(m: Map[String, Any]): String = render(m)

  private def pretty(v: Any, indent: Int = 0): String = {
    val pad = "  " * indent
    val pad1 = "  " * (indent + 1)
    v match {
      case m: Map[_, _] =>
        val body = m
          .asInstanceOf[Map[String, Any]]
          .toSeq
          .map { case (k, x) => s"""$pad1"$k": ${pretty(x, indent + 1)}""" }
          .mkString(",\n")
        s"{\n$body\n$pad}"
      case s: Iterable[_] if !s.isInstanceOf[Map[_, _]] =>
        if (s.isEmpty) "[]"
        else s.map(x => s"$pad1${pretty(x, indent + 1)}").mkString("[\n", ",\n", s"\n$pad]")
      case other => render(other)
    }
  }
}
