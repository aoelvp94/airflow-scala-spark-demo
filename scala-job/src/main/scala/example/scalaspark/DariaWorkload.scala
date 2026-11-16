package example.scalaspark

import com.github.mrpowers.spark.daria.sql.DariaValidator.{
  validateAbsenceOfColumns,
  validatePresenceOfColumns,
  validateSchema
}
import com.github.mrpowers.spark.daria.sql.functions.singleSpace
import com.github.mrpowers.spark.daria.sql.transformations.camelCaseToSnakeCaseColumns
import com.github.mrpowers.spark.daria.sql.{ColumnExt, DataFrameExt}
import org.apache.spark.sql.functions.{col, count, lit, regexp_replace, sum, trim, when}
import org.apache.spark.sql.types.{BooleanType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object DariaWorkload {
  val RequiredColumns: Seq[String] =
    Seq("user_id", "group_key", "order_amount", "is_active", "display_name")
  val ProhibitedColumns: Seq[String] =
    Seq("userId", "groupKey", "orderAmount", "isActive", "displayName")

  val RequiredSchema: StructType = StructType(
    Seq(
      StructField("user_id", LongType, nullable = false),
      StructField("group_key", LongType, nullable = true),
      StructField("order_amount", LongType, nullable = false),
      StructField("is_active", BooleanType, nullable = true),
      StructField("display_name", StringType, nullable = false)
    )
  )

  def source(spark: SparkSession, rows: Long = Workload.Rows): DataFrame =
    spark
      .range(0L, rows, 1L, 8)
      .selectExpr(
        "id as userId",
        "id % 1000 as groupKey",
        "id * 2 as orderAmount",
        "cast(id % 2 as boolean) as isActive",
        "concat('  User  ', id, '  ') as displayName"
      )

  def withDaria(df: DataFrame): DataFrame = {
    import ColumnExt._
    import DataFrameExt._
    val renamed = df.transform(camelCaseToSnakeCaseColumns())
    validatePresenceOfColumns(renamed, RequiredColumns)
    validateAbsenceOfColumns(renamed, ProhibitedColumns)
    validateSchema(renamed, RequiredSchema)
    renamed.composeTransforms(
      (d: DataFrame) => d.withColumn("display_name", singleSpace(col("display_name"))),
      (d: DataFrame) => d.withColumn("is_active_truthy", col("is_active").isTruthy)
    )
  }

  def withPlain(df: DataFrame): DataFrame = {
    val renamed = df.columns.foldLeft(df) { (memo, name) =>
      memo.withColumnRenamed(name, camelToSnake(name))
    }
    val missing = RequiredColumns.diff(renamed.columns.toSeq)
    if (missing.nonEmpty) {
      throw new IllegalArgumentException(
        s"The [${missing.mkString(", ")}] columns are not included in the DataFrame with the following columns [${renamed.columns.mkString(", ")}]"
      )
    }
    val presentProhibited = ProhibitedColumns.filter(renamed.columns.contains)
    if (presentProhibited.nonEmpty) {
      throw new IllegalArgumentException(
        s"The [${presentProhibited.mkString(", ")}] columns are not allowed to be included in the DataFrame with the following columns [${renamed.columns.mkString(", ")}]"
      )
    }
    val missingFields = RequiredSchema.filterNot(f => renamed.schema.fields.exists(_ == f))
    if (missingFields.nonEmpty) {
      throw new IllegalArgumentException(
        s"The [${missingFields.mkString(", ")}] StructFields are not included in the DataFrame with the following StructFields [${renamed.schema}]"
      )
    }
    renamed
      .withColumn("display_name", trim(regexp_replace(col("display_name"), " +", " ")))
      .withColumn(
        "is_active_truthy",
        when(col("is_active").isNull || col("is_active") === false, false).otherwise(true)
      )
  }

  def camelToSnake(name: String): String =
    name.replaceAll("([A-Z]+)", "_$1").toLowerCase.stripPrefix("_")

  def aggregate(spark: SparkSession, df: DataFrame, out: String): (Long, Long, Long) = {
    df.groupBy("group_key")
      .agg(
        sum("order_amount").as("s"),
        count(lit(1)).as("c"),
        sum(when(col("is_active_truthy"), 1).otherwise(0)).as("active_c")
      )
      .write
      .mode("overwrite")
      .parquet(out)
    val row = spark.read.parquet(out).agg(sum("c"), count(lit(1)), sum("s")).first()
    (row.getLong(0), row.getLong(1), row.getLong(2))
  }

  def compute(spark: SparkSession, mode: String, rows: Long = Workload.Rows): (Long, Long, Long) = {
    val src = source(spark, rows)
    val transformed = mode match {
      case "plain" => withPlain(src)
      case _       => withDaria(src)
    }
    aggregate(spark, transformed, s"/tmp/bench_out/$mode")
  }

  def run(mode: String, runId: String, rows: Long = Workload.Rows): WorkloadResult = {
    val t0 = System.nanoTime()
    val spark = Workload.session(s"bench-$mode")
    val t1 = System.nanoTime()
    try {
      val (n, groups, checksum) = compute(spark, mode, rows)
      val t2 = System.nanoTime()
      val r = WorkloadResult(mode, n, groups, checksum, (t1 - t0) / 1000000, (t2 - t1) / 1000000, Workload.vmHwmKb())
      Workload.writeResult(r, runId)
      r
    } finally spark.stop()
  }
}
