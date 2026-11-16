package example.scalaspark

import com.github.mrpowers.spark.daria.sql.{InvalidDataFrameSchemaException, MissingDataFrameColumnsException, ProhibitedDataFrameColumnsException}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class DariaWorkloadTest extends AnyFunSuite with BeforeAndAfterAll {
  private lazy val spark = Workload.session("daria-test")

  override def afterAll(): Unit = spark.stop()

  test("spark-daria 1.2.4 runs on Spark 4.3 local[*]") {
    val df = DariaWorkload.withDaria(DariaWorkload.source(spark, 1000L))
    assert(spark.version.startsWith("4.3"))
    assert(df.columns.toSeq == Seq("user_id", "group_key", "order_amount", "is_active", "display_name", "is_active_truthy"))
    assert(df.count() == 1000L)
    assert(df.filter(col("display_name") === "User 0").count() == 1L)
    assert(df.filter(col("is_active_truthy")).count() == 500L)
  }

  test("validatePresenceOfColumns fails when a required column is missing") {
    val df = DariaWorkload.source(spark, 10L).drop("userId")
    intercept[MissingDataFrameColumnsException] {
      DariaWorkload.withDaria(df)
    }
  }

  test("validateSchema fails when a required field is missing") {
    import com.github.mrpowers.spark.daria.sql.DariaValidator.validateSchema
    val df = DariaWorkload.withPlain(DariaWorkload.source(spark, 10L))
    intercept[InvalidDataFrameSchemaException] {
      validateSchema(df, StructType(Seq(StructField("missing", IntegerType, nullable = true))))
    }
  }

  test("validateAbsenceOfColumns fails if camelCase names are left in place") {
    intercept[ProhibitedDataFrameColumnsException] {
      com.github.mrpowers.spark.daria.sql.DariaValidator
        .validateAbsenceOfColumns(DariaWorkload.source(spark, 10L), DariaWorkload.ProhibitedColumns)
    }
  }

  test("daria and plain pipelines produce the same schema, rows, groups and checksum") {
    val src = DariaWorkload.source(spark, 5000L)
    val daria = DariaWorkload.withDaria(src)
    val plain = DariaWorkload.withPlain(src)
    assert(daria.schema == plain.schema)
    val (dRows, dGroups, dSum) = DariaWorkload.aggregate(spark, daria, "/tmp/bench_out/test-daria")
    val (pRows, pGroups, pSum) = DariaWorkload.aggregate(spark, plain, "/tmp/bench_out/test-plain")
    assert((dRows, dGroups, dSum) == (pRows, pGroups, pSum))
    assert((dRows, dGroups) == (5000L, 1000L))
  }
}
