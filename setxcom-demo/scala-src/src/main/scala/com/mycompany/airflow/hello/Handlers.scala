package com.mycompany.airflow.hello

import org.apache.airflow.sdk.{Bundle, Client, Context, InputTask, Server, Task}

import scala.jdk.CollectionConverters._

final case class ScoreSummary(region: String, threshold: Double)

class Extract extends Task {
  override def execute(context: Context, client: Client): Unit =
    client.setXCom(java.lang.Long.valueOf(42L))
}

class Score extends InputTask[ScoreInput] {
  override def execute(context: Context, client: Client, input: ScoreInput): Unit = {
    val summary = Map("region" -> input.region, "threshold" -> input.threshold).asJava
    client.setXCom(summary)
  }
}

class PushAsJava extends Task {
  override def execute(context: Context, client: Client): Unit = {
    val summary = Map("region" -> "eu", "threshold" -> 0.5).asJava
    client.setXCom(summary)
  }
}

class PushJavaPojo extends Task {
  override def execute(context: Context, client: Client): Unit =
    client.setXCom(new ScoreSummaryBean("eu", 0.5))
}

class PushScalaMap extends Task {
  override def execute(context: Context, client: Client): Unit =
    client.setXCom(Map("region" -> "eu", "threshold" -> 0.5))
}

class PushScalaSeq extends Task {
  override def execute(context: Context, client: Client): Unit =
    client.setXCom(Seq(1, 2, 3))
}

class PushScalaOption extends Task {
  override def execute(context: Context, client: Client): Unit =
    client.setXCom(Some("eu"))
}

class PushCaseClass extends Task {
  override def execute(context: Context, client: Client): Unit =
    client.setXCom(ScoreSummary("eu", 0.5))
}

class PushEmptyObject extends Task {
  override def execute(context: Context, client: Client): Unit =
    client.setXCom(new EmptyMarker())
}

object Main {
  def main(args: Array[String]): Unit =
    Server.create(args).serve(
      new Bundle()
        .register("setxcom_fix", "extract", classOf[Extract])
        .register("setxcom_fix", "score", classOf[Score])
        .register("setxcom_fix", "push_as_java", classOf[PushAsJava])
        .register("setxcom_fix", "push_java_pojo", classOf[PushJavaPojo])
        .register("setxcom_fix", "push_scala_map", classOf[PushScalaMap])
        .register("setxcom_fix", "push_scala_seq", classOf[PushScalaSeq])
        .register("setxcom_fix", "push_scala_option", classOf[PushScalaOption])
        .register("setxcom_fix", "push_case_class", classOf[PushCaseClass])
        .register("setxcom_fix", "push_empty_object", classOf[PushEmptyObject])
    )
}
