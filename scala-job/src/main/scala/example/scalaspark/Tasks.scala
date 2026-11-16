package example.scalaspark

import org.apache.airflow.sdk.{Client, Context, Task}
import org.apache.logging.log4j.{LogManager, Logger}

import scala.jdk.CollectionConverters._

class SparkWorkload extends Task {
  private val log: Logger = LogManager.getLogger(classOf[SparkWorkload])

  override def execute(context: Context, client: Client): Unit = {
    val r = Workload.run("java_sdk", context.ti.runId)
    log.info("BENCH_RESULT {}", r.toJson)
    client.setXCom(r.toJava)
  }
}

class SparkExtract extends Task {
  private val log: Logger = LogManager.getLogger(classOf[SparkExtract])

  override def execute(context: Context, client: Client): Unit = {
    val spark = Workload.session("etl-extract")
    try {
      val n = spark.range(0L, 1000000L).count()
      log.info("Extracted {} rows", n)
      client.setXCom(n)
    } finally spark.stop()
  }
}

class SparkAggregate extends Task {
  private val log: Logger = LogManager.getLogger(classOf[SparkAggregate])

  override def execute(context: Context, client: Client): Unit = {
    val upstream = client.getXCom("extract").asInstanceOf[Number].longValue()
    val spark = Workload.session("etl-aggregate")
    try {
      val total = spark.range(0L, upstream).selectExpr("sum(id) as s").first().getLong(0)
      log.info("Sum over {} rows is {}", upstream, total)
      client.setXCom(Map("rows" -> upstream, "total" -> total).asJava)
    } finally spark.stop()
  }
}

class Report extends Task {
  private val log: Logger = LogManager.getLogger(classOf[Report])

  override def execute(context: Context, client: Client): Unit = {
    val m = client.getXCom("aggregate").asInstanceOf[java.util.Map[String, Any]].asScala
    println(s"stdout from Scala: report=$m")
    System.err.println("stderr from Scala")
    log.info("Report {}", m)
    require(m("total").asInstanceOf[Number].longValue() == 499999500000L, s"unexpected total $m")
  }
}

final case class Point(x: Int, y: Int)

class PushScalaMap extends Task {
  override def execute(context: Context, client: Client): Unit = client.setXCom(Map("a" -> 1, "b" -> 2))
}

class PushScalaSeq extends Task {
  override def execute(context: Context, client: Client): Unit = client.setXCom(Seq(1, 2, 3))
}

class PushCaseClass extends Task {
  override def execute(context: Context, client: Client): Unit = client.setXCom(Point(1, 2))
}

class PushScalaOption extends Task {
  override def execute(context: Context, client: Client): Unit = client.setXCom(Some(1L))
}

class PushJavaMap extends Task {
  override def execute(context: Context, client: Client): Unit =
    client.setXCom(Map[String, Any]("a" -> 1, "nested" -> Seq(1, 2).asJava).asJava)
}

class PushBigDecimal extends Task {
  override def execute(context: Context, client: Client): Unit = client.setXCom(BigDecimal("1.5"))
}

class ReadXComs extends Task {
  private val log: Logger = LogManager.getLogger(classOf[ReadXComs])

  override def execute(context: Context, client: Client): Unit =
    Seq("push_scala_map", "push_scala_seq", "push_case_class", "push_scala_option", "push_java_map", "push_big_decimal")
      .foreach { t =>
        val v =
          try {
            val x = client.getXCom(t)
            s"${if (x == null) "null" else x.getClass.getName} -> $x"
          } catch { case e: Throwable => s"error ${e.getClass.getName}: ${e.getMessage}" }
        log.info("xcom {} = {}", t, v)
      }
}

class Noop extends Task {
  override def execute(context: Context, client: Client): Unit = client.setXCom(Workload.vmHwmKb())
}
