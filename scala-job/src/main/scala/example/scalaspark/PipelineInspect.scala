package example.scalaspark

import com.github.mrpowers.spark.daria.sql.MissingDataFrameColumnsException

object PipelineInspect {
  def main(args: Array[String]): Unit = {
    System.setProperty("log4j.configurationFile", "log4j2-local.xml")
    val spark = Workload.session("pipeline-inspect")
    try {
      val src = Pipeline.bronzeFrame(spark, 40L)
      println("BRONZE_SCHEMA")
      src.printSchema()
      println("BRONZE_ROWS")
      src.show(5, truncate = false)
      try {
        Pipeline.withDaria(src.drop("OrderID"))
        println("MISSING_COLUMN: no exception")
      } catch {
        case e: MissingDataFrameColumnsException =>
          println("MISSING_COLUMN_EXCEPTION: " + e.getClass.getName)
          println("MISSING_COLUMN_MESSAGE: " + e.getMessage)
      }
      val silver = Pipeline.withDaria(src)
      println("SILVER_SCHEMA")
      silver.printSchema()
      println("SILVER_ROWS")
      silver.orderBy("order_id").show(5, truncate = false)
      println(s"SILVER_COUNT=${silver.count()} CHECKSUM=${Pipeline.checksum(silver)}")
    } finally spark.stop()
  }
}
