package example.scalaspark

import org.apache.airflow.sdk.{Bundle, Server}

object ScalaBundle {
  def build(): Bundle =
    new Bundle()
      .register("bench_scala_java_sdk", "workload", classOf[SparkWorkload])
      .register("scala_spark_stub_etl", "extract", classOf[SparkExtract])
      .register("scala_spark_stub_etl", "aggregate", classOf[SparkAggregate])
      .register("scala_spark_stub_etl", "report", classOf[Report])
      .register("scala_sdk_xcom_ergonomics", "push_scala_map", classOf[PushScalaMap])
      .register("scala_sdk_xcom_ergonomics", "push_scala_seq", classOf[PushScalaSeq])
      .register("scala_sdk_xcom_ergonomics", "push_case_class", classOf[PushCaseClass])
      .register("scala_sdk_xcom_ergonomics", "push_scala_option", classOf[PushScalaOption])
      .register("scala_sdk_xcom_ergonomics", "push_java_map", classOf[PushJavaMap])
      .register("scala_sdk_xcom_ergonomics", "push_big_decimal", classOf[PushBigDecimal])
      .register("scala_sdk_xcom_ergonomics", "read_xcoms", classOf[ReadXComs])
      .register("bench_noop", "scala_noop", classOf[Noop])
      .register("scala_spark_daria", "daria", classOf[SparkDariaWorkload])
      .register("scala_spark_daria", "plain", classOf[SparkPlainWorkload])
      .register("bench_scala_daria_java_sdk", "workload", classOf[SparkDariaWorkload])
      .register("bench_scala_plain_java_sdk", "workload", classOf[SparkPlainWorkload])
      .register(NativeDag.build())
      .register(DariaNativeDag.build())
      .register("scala_spark_ingest_transform", "ingest", classOf[SparkIngest])
      .register("scala_spark_ingest_transform", "transform", classOf[SparkTransform])
      .register(IngestTransformNativeDag.build())
}

object BundleMain {
  def main(args: Array[String]): Unit = Server.create(args).serve(ScalaBundle.build())
}

object SubmitApp {
  def main(args: Array[String]): Unit = {
    val mode = args.headOption.getOrElse("spark_submit")
    val runId = args.lift(1).getOrElse("manual")
    val result = mode match {
      case "daria" | "plain" => DariaWorkload.run(mode, runId)
      case _                 => Workload.run(mode, runId)
    }
    println(s"BENCH_RESULT ${result.toJson}")
  }
}
