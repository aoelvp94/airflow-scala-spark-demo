package example.scalaspark

import com.github.mrpowers.spark.daria.sql.MissingDataFrameColumnsException
import com.github.mrpowers.spark.daria.sql.transformations.{sortColumns, titleCaseColumns}
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class PipelineTest extends AnyFunSuite with BeforeAndAfterAll {
  private lazy val spark = Workload.session("pipeline-test")

  override def afterAll(): Unit = spark.stop()

  test("HTTP ingest writes messy nested bronze and daria builds silver and gold") {
    val root = "/tmp/pipeline/test-medallion"
    val unique = 200L
    val ingested = Pipeline.ingest(spark, s"$root/bronze", unique, pageSize = 50)
    assert(ingested.pages == 5)
    assert(ingested.rows == Pipeline.bronzeCount(unique))
    assert(ingested.uniqueIds == unique)

    val raw = spark.read.schema(Pipeline.BronzeSchema).parquet(ingested.bronzePath)
    assert(raw.columns.toSeq == Pipeline.BronzeColumns)
    assert(raw.schema("Address").dataType.typeName == "struct")
    assert(raw.schema("LineItems").dataType.typeName == "array")
    assert(raw.filter(col("AMOUNT_USD").contains(",")).count() > 0)

    val result = Pipeline.transform(spark, ingested.bronzePath, s"$root/silver", "daria")
    assert(result.silverRows == unique)
    val out = spark.read.parquet(result.silverPath)
    assert(out.columns.toSeq.sorted == Pipeline.SilverColumns.sorted)
    assert(out.filter(col("customer_name") === "User 1").count() == 1L)
    assert(out.filter(col("email_address") === "user1@example.com").count() == 1L)
    assert(out.filter(col("address_city") === "Buenos Aires").count() >= 1L)
    assert(out.filter(col("line_item_count") === 2).count() >= 1L)
    assert(out.filter(col("order_date").isNull).count() == 0L)

    val gold = Pipeline.toGold(spark, result.silverPath, s"$root/gold")
    assert(gold.revenueRows > 0)
    assert(gold.segmentRows > 0)
    assert(gold.kpis("orders").asInstanceOf[Long] == unique)
    assert(gold.kpis("revenue").asInstanceOf[Double] > 0)
  }

  test("validatePresenceOfColumns fails clearly when a bronze column is missing") {
    val df = Pipeline.bronzeFrame(spark, 20L).drop("OrderID")
    val ex = intercept[MissingDataFrameColumnsException] {
      Pipeline.withDaria(df)
    }
    assert(ex.getMessage.contains("OrderID"))
  }

  test("daria and plain silver transforms agree on rows and checksum") {
    val src = Pipeline.bronzeFrame(spark, 500L)
    val daria = Pipeline.withDaria(src)
    val plain = Pipeline.withPlain(src)
    assert(daria.count() == 500L)
    assert(daria.count() == plain.count())
    assert(Pipeline.checksum(daria) == Pipeline.checksum(plain))
  }

  test("duplicates are dropped and string amounts, dates, flags and addresses are cleaned") {
    val df = Pipeline.withDaria(Pipeline.bronzeFrame(spark, 100L))
    assert(df.count() == 100L)
    val row1 = df.filter(col("order_id") === 10001L).collect().head
    assert(row1.getAs[String]("customer_name") == "User 1")
    assert(row1.getAs[String]("email_address") == "user1@example.com")
    assert(!row1.isNullAt(row1.fieldIndex("order_date")))
    assert(row1.getAs[Boolean]("is_vip"))
    assert(row1.getAs[String]("address_city") == "Cordoba")
    assert(row1.getAs[String]("address_postal_code") != null)
    assert(!row1.getAs[String]("address_postal_code").contains(" "))
    val row4 = df.filter(col("order_id") === 10004L).collect().head
    assert(!row4.getAs[Boolean]("is_vip"))
    val row0 = df.filter(col("order_id") === 10000L).collect().head
    assert(row0.getAs[String]("customer_name") == null)
  }

  test("titleCaseColumns is available and renames snake_case headers") {
    val df = Pipeline.withDaria(Pipeline.bronzeFrame(spark, 5L)).select("customer_name")
    val titled = df.transform(titleCaseColumns())
    assert(titled.columns.head.toLowerCase.contains("customer"))
    val sorted = df.transform(sortColumns("asc"))
    assert(sorted.columns.toSeq == df.columns.toSeq.sorted)
  }

  test("lineage listener records parquet inputs, outputs and column links") {
    val root = "/tmp/pipeline/test-lineage"
    val lineageDir = s"$root/lineage"
    new java.io.File(lineageDir).mkdirs()
    val eventsFile = java.nio.file.Paths.get(lineageDir, "events.jsonl")
    java.nio.file.Files.deleteIfExists(eventsFile)
    spark.listenerManager.register(new FileLineageListener(lineageDir, "lineage-test"))
    val bronzePath = s"$root/bronze"
    val silverPath = s"$root/silver"
    Pipeline.bronzeFrame(spark, 40L).write.mode("overwrite").parquet(bronzePath)
    val _ = Pipeline.transform(spark, bronzePath, silverPath, "daria")
    val events = Lineage.readEvents(lineageDir)
    assert(events.nonEmpty, "expected at least one write event")
    val complete = events.filter(_.get("eventType") == "COMPLETE")
    assert(complete.nonEmpty)
    val hasSilver = complete.exists { e =>
      Option(e.get("outputs")).exists(_.toString.contains("silver"))
    }
    assert(hasSilver)
    val hasColumns = complete.exists { e =>
      val cols = e.get("columnLineage")
      cols != null && cols.toString.contains("order_id")
    }
    assert(hasColumns)
  }
}
