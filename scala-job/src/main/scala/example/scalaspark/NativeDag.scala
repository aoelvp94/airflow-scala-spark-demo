package example.scalaspark

import org.apache.airflow.sdk.{Client, Context, DagDef, Task}
import org.apache.logging.log4j.{LogManager, Logger}

import scala.jdk.CollectionConverters._

class CheckWorkload extends Task {
  private val log: Logger = LogManager.getLogger(classOf[CheckWorkload])

  override def execute(context: Context, client: Client): Unit = {
    val m = client.getXCom("workload").asInstanceOf[java.util.Map[String, Any]].asScala
    log.info("Native Scala Dag got {}", m)
    require(m("groups").asInstanceOf[Number].longValue() == 1000L)
  }
}

object NativeDag {
  def build(): DagDef = {
    val dag = new DagDef("scala_native_spark")
      .config("description", "Dag declared in Scala with the Java SDK DagDef API")
      .config("queue", "scala")
      .config("catchup", false)
      .config("tags", Seq("scala", "spark", "native").asJava)
    val workload = dag.task("workload", classOf[SparkWorkload])
    val check = dag.task("check", classOf[CheckWorkload])
    check.after(workload)
    dag
  }
}

class CheckDariaWorkload extends Task {
  private val log: Logger = LogManager.getLogger(classOf[CheckDariaWorkload])

  override def execute(context: Context, client: Client): Unit = {
    val m = client.getXCom("daria").asInstanceOf[java.util.Map[String, Any]].asScala
    log.info("Native Scala daria Dag got {}", m)
    require(m("groups").asInstanceOf[Number].longValue() == 1000L)
    require(m("rows").asInstanceOf[Number].longValue() == Workload.Rows)
  }
}

object DariaNativeDag {
  def build(): DagDef = {
    val dag = new DagDef("scala_native_daria")
      .config("description", "Native Scala Dag that validates and transforms a Spark DataFrame with spark-daria")
      .config("queue", "scala")
      .config("catchup", false)
      .config("tags", Seq("scala", "spark", "daria", "native").asJava)
    val daria = dag.task("daria", classOf[SparkDariaWorkload])
    val check = dag.task("check", classOf[CheckDariaWorkload])
    check.after(daria)
    dag
  }
}

class CheckMedallion extends Task {
  private val log: Logger = LogManager.getLogger(classOf[CheckMedallion])

  override def execute(context: Context, client: Client): Unit = {
    val m = client.getXCom("silver_to_gold").asInstanceOf[java.util.Map[String, Any]].asScala
    log.info("Native medallion Dag got {}", m)
    require(m("revenue_rows").asInstanceOf[Number].longValue() > 0)
    require(m("segment_rows").asInstanceOf[Number].longValue() > 0)
  }
}

object MedallionNativeDag {
  def build(): DagDef = {
    val dag = new DagDef("scala_native_medallion")
      .config(
        "description",
        "Native Scala Dag: HTTP ingest to bronze, spark-daria silver, then gold aggregates"
      )
      .config("queue", "scala")
      .config("catchup", false)
      .config("tags", Seq("scala", "spark", "daria", "medallion", "native").asJava)
    val ingest = dag.task("ingest", classOf[SparkIngest])
    val silver = dag.task("bronze_to_silver", classOf[SparkBronzeToSilver])
    val gold = dag.task("silver_to_gold", classOf[SparkSilverToGold])
    val check = dag.task("check", classOf[CheckMedallion])
    silver.after(ingest)
    gold.after(silver)
    check.after(gold)
    dag
  }
}
