package example.scalaspark

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, lit, sum}

import scala.jdk.CollectionConverters._

final case class WorkloadResult(
    mode: String,
    rows: Long,
    groups: Long,
    checksum: Long,
    sparkStartMs: Long,
    computeMs: Long,
    jvmHwmKb: Long
) {
  def fields: Seq[(String, Any)] = Seq(
    "mode" -> mode,
    "rows" -> rows,
    "groups" -> groups,
    "checksum" -> checksum,
    "spark_start_ms" -> sparkStartMs,
    "compute_ms" -> computeMs,
    "jvm_hwm_kb" -> jvmHwmKb
  )

  def toJava: java.util.Map[String, Any] = fields.toMap.asJava

  def toJson: String = fields
    .map {
      case (k, v: String) => s""""$k": "$v""""
      case (k, v) => s""""$k": $v"""
    }
    .mkString("{", ", ", "}")
}

object Workload {
  val Rows: Long = sys.env.get("BENCH_ROWS").map(_.toLong).getOrElse(20000000L)

  def session(app: String): SparkSession =
    SparkSession
      .builder()
      .appName(app)
      .master(sys.env.getOrElse("BENCH_SPARK_MASTER", "local[*]"))
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

  def compute(spark: SparkSession, mode: String): (Long, Long, Long) = {
    val out = s"/tmp/bench_out/$mode"
    spark
      .range(0L, Rows, 1L, 8)
      .selectExpr("id % 1000 as k", "id * 2 as v")
      .groupBy("k")
      .agg(sum("v").as("s"), count(lit(1)).as("c"))
      .write
      .mode("overwrite")
      .parquet(out)
    val row = spark.read.parquet(out).agg(sum("c"), count(lit(1)), sum("s")).first()
    (row.getLong(0), row.getLong(1), row.getLong(2))
  }

  def run(mode: String, runId: String): WorkloadResult = {
    val t0 = System.nanoTime()
    val spark = session(s"bench-$mode")
    val t1 = System.nanoTime()
    try {
      val (rows, groups, checksum) = compute(spark, mode)
      val t2 = System.nanoTime()
      val r = WorkloadResult(mode, rows, groups, checksum, (t1 - t0) / 1000000, (t2 - t1) / 1000000, vmHwmKb())
      writeResult(r, runId)
      r
    } finally spark.stop()
  }

  def vmHwmKb(): Long =
    Files
      .readAllLines(Paths.get("/proc/self/status"))
      .asScala
      .find(_.startsWith("VmHWM:"))
      .map(_.replaceAll("[^0-9]", "").toLong)
      .getOrElse(-1L)

  def writeResult(r: WorkloadResult, runId: String): Unit = {
    val dir = Paths.get("/tmp/bench_results", r.mode)
    Files.createDirectories(dir)
    val safe = runId.replaceAll("[^A-Za-z0-9_.-]", "_")
    Files.write(dir.resolve(s"$safe.json"), r.toJson.getBytes(StandardCharsets.UTF_8))
  }
}
