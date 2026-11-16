ThisBuild / scalaVersion := "2.13.18"
ThisBuild / organization := "example"
ThisBuild / version := "0.1.0"

val airflowSdkVersion = "1.0.0"
val sparkVersion = sys.env.getOrElse("SPARK_VERSION", "4.3.0")
val bundleMainClass = "example.scalaspark.BundleMain"
val jacksonAlign = sys.env.getOrElse("JACKSON_ALIGN", "up")
val log4jAlign = sys.env.getOrElse("LOG4J_ALIGN", "2.26.1")

val jacksonOverrides = jacksonAlign match {
  case "up" => Seq("com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.22.2")
  case "down" =>
    Seq("jackson-core", "jackson-databind").map("com.fasterxml.jackson.core" % _ % "2.21.2") ++ Seq(
      "com.fasterxml.jackson.core" % "jackson-annotations" % "2.21",
      "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.21.2",
      "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.21.2"
    )
  case _ => Seq.empty
}
val log4jOverrides =
  if (log4jAlign == "none") Seq.empty
  else Seq("log4j-api", "log4j-core", "log4j-1.2-api", "log4j-slf4j2-impl", "log4j-layout-template-json")
    .map("org.apache.logging.log4j" % _ % log4jAlign)

def sparkJavaOptsFrom(base: File): Seq[String] = {
  val f = base.getParentFile / "spark-jvm-opts.txt"
  if (f.exists) IO.readLines(f).map(_.trim).filter(_.nonEmpty) else Seq.empty
}

lazy val root = (project in file("."))
  .settings(
    name := "scala-spark-airflow",
    javacOptions ++= Seq("--release", "17"),
    scalacOptions ++= Seq("-release", "17", "-deprecation"),
    libraryDependencies ++= Seq(
      "org.apache.airflow" % "airflow-sdk" % airflowSdkVersion,
      "org.apache.airflow" % "airflow-sdk-log4j2" % airflowSdkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "com.github.mrpowers" %% "spark-daria" % "1.2.4",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    ),
    Test / fork := true,
    Test / parallelExecution := false,
    Test / javaOptions ++= sparkJavaOptsFrom(baseDirectory.value) ++ Seq("-Xmx1g"),
    run / fork := true,
    run / javaOptions ++= sparkJavaOptsFrom(baseDirectory.value) ++ Seq("-Xmx1g"),
    dependencyOverrides ++= jacksonOverrides ++ log4jOverrides,
    Compile / mainClass := Some(bundleMainClass),
    Compile / packageBin / packageOptions +=
      Package.ManifestAttributes("Main-Class" -> bundleMainClass),
    bundle := {
      val out = target.value / "bundle"
      IO.delete(out)
      IO.createDirectory(out)
      val app = (Compile / packageBin).value
      IO.copyFile(app, out / app.getName)
      (Runtime / fullClasspathAsJars).value.map(_.data).filter(_.getName.endsWith(".jar"))
        .filterNot(_ == app)
        .foreach(j => IO.copyFile(j, out / j.getName))
      out
    },
    submitJar := {
      val out = target.value / "submit"
      IO.delete(out)
      IO.createDirectory(out)
      val app = (Compile / packageBin).value
      IO.copyFile(app, out / "scala-spark-workload.jar")
      (Runtime / fullClasspathAsJars).value
        .map(_.data)
        .filter(j => j.getName.startsWith("spark-daria"))
        .foreach(j => IO.copyFile(j, out / j.getName))
      out
    },
    assembly / mainClass := Some(bundleMainClass),
    assembly / assemblyJarName := "scala-spark-airflow-fat.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) if xs.lastOption.exists(n => n.endsWith(".SF") || n.endsWith(".DSA") || n.endsWith(".RSA")) => MergeStrategy.discard
      case PathList("META-INF", "versions", _*) => MergeStrategy.first
      case "module-info.class" => MergeStrategy.discard
      case x if x.endsWith("/module-info.class") => MergeStrategy.discard
      case x if x.endsWith("Log4j2Plugins.dat") => MergeStrategy.first
      case "reference.conf" => MergeStrategy.concat
      case _ => MergeStrategy.first
    }
  )

lazy val bundle = taskKey[File]("Thin Airflow bundle: app jar plus every runtime jar in target/bundle")
lazy val submitJar = taskKey[File]("App jar plus spark-daria, for spark-submit with Spark provided")
