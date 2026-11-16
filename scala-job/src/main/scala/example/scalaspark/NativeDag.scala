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

class CheckIngestTransform extends Task {
  private val log: Logger = LogManager.getLogger(classOf[CheckIngestTransform])

  override def execute(context: Context, client: Client): Unit = {
    val m = client.getXCom("transform").asInstanceOf[java.util.Map[String, Any]].asScala
    log.info("Native ingest/transform Dag got {}", m)
    require(m("silver_rows").asInstanceOf[Number].longValue() == Pipeline.DefaultUniqueRows)
    require(m("bronze_rows").asInstanceOf[Number].longValue() == Pipeline.bronzeCount(Pipeline.DefaultUniqueRows))
  }
}

object IngestTransformNativeDag {
  def build(): DagDef = {
    val dag = new DagDef("scala_native_ingest_transform")
      .config(
        "description",
        "Native Scala Dag: ingest messy orders from a paginated HTTP API, then transform bronze to silver with spark-daria"
      )
      .config("queue", "scala")
      .config("catchup", false)
      .config("tags", Seq("scala", "spark", "daria", "ingest", "native").asJava)
    val ingest = dag.task("ingest", classOf[SparkIngest])
    val transform = dag.task("transform", classOf[SparkTransform])
    val check = dag.task("check", classOf[CheckIngestTransform])
    transform.after(ingest)
    check.after(transform)
    dag
  }
}
