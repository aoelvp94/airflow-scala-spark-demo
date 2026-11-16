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
