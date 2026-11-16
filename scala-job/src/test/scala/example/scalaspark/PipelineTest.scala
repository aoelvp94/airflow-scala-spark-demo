package example.scalaspark

import com.github.mrpowers.spark.daria.sql.MissingDataFrameColumnsException
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class PipelineTest extends AnyFunSuite with BeforeAndAfterAll {
  private lazy val spark = Workload.session("pipeline-test")

  override def afterAll(): Unit = spark.stop()

  test("HTTP ingest writes messy bronze and the daria transform cleans it to silver") {
    val root = "/tmp/pipeline/test-http"
    val bronze = s"$root/bronze"
    val silver = s"$root/silver"
    val unique = 200L
    val ingested = Pipeline.ingest(spark, bronze, unique, pageSize = 50)
    assert(ingested.pages == 5)
    assert(ingested.rows == Pipeline.bronzeCount(unique))
    assert(ingested.uniqueIds == unique)

    val raw = spark.read.schema(Pipeline.BronzeSchema).parquet(bronze)
    assert(raw.columns.toSeq == Pipeline.BronzeColumns)
    assert(raw.filter(col("Customer Name").startsWith("  ")).count() > 0)
    assert(raw.filter(col("AMOUNT_USD").contains(",")).count() > 0)

    val result = Pipeline.transform(spark, bronze, silver, "daria")
    assert(result.bronzeRows == ingested.rows)
    assert(result.silverRows == unique)

    val out = spark.read.parquet(silver)
    assert(out.columns.toSeq == Pipeline.SilverColumns)
    assert(out.filter(col("customer_name") === "User 1").count() == 1L)
    assert(out.filter(col("email_address") === "user1@example.com").count() == 1L)
    assert(out.filter(col("country_code") === "AR").count() >= 1L)
    assert(out.filter(col("notes") === "packed and ready").count() >= 1L)
    assert(out.filter(col("order_date").isNull).count() == 0L)
  }

  test("validatePresenceOfColumns fails clearly when a bronze column is missing") {
    val df = Pipeline.bronzeFrame(spark, 20L).drop("OrderID")
    val ex = intercept[MissingDataFrameColumnsException] {
      Pipeline.withDaria(df)
    }
    assert(ex.getMessage.contains("OrderID"))
  }

  test("daria and plain transforms agree on schema, rows and checksum") {
    val src = Pipeline.bronzeFrame(spark, 500L)
    val daria = Pipeline.withDaria(src)
    val plain = Pipeline.withPlain(src)
    assert(daria.schema == plain.schema)
    assert(daria.columns.toSeq == Pipeline.SilverColumns)
    assert(daria.count() == 500L)
    assert(Pipeline.checksum(daria) == Pipeline.checksum(plain))
  }

  test("duplicates are dropped and string amounts, dates and flags are cleaned") {
    val df = Pipeline.withDaria(Pipeline.bronzeFrame(spark, 100L))
    assert(df.count() == 100L)
    val row1 = df.filter(col("order_id") === 10001L).collect().head
    assert(row1.getAs[String]("customer_name") == "User 1")
    assert(row1.getAs[String]("email_address") == "user1@example.com")
    assert(!row1.isNullAt(row1.fieldIndex("order_date")))
    assert(row1.getAs[Boolean]("is_vip"))
    val row4 = df.filter(col("order_id") === 10004L).collect().head
    assert(!row4.getAs[Boolean]("is_vip"))
    val row0 = df.filter(col("order_id") === 10000L).collect().head
    assert(row0.getAs[String]("customer_name") == null)
  }
}
