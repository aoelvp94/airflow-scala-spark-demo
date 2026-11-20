ThisBuild / scalaVersion := "2.13.18"

val airflowSdkVersion = "1.0.0"
val bundleMainClass = "com.mycompany.airflow.hello.Main"

lazy val bundle = taskKey[File]("Write a thin Java SDK bundle directory")

lazy val root = (project in file("."))
  .settings(
    name := "setxcom-reviewer-demo",
    javacOptions ++= Seq("--release", "17"),
    scalacOptions ++= Seq("-release", "17"),
    libraryDependencies += "org.apache.airflow" % "airflow-sdk" % airflowSdkVersion,
    // Same Jackson minor as airflow-sdk 1.0.0 (2.22), so the module registers cleanly.
    libraryDependencies ++= (
      if (sys.env.get("WITH_JACKSON_SCALA").contains("1"))
        Seq("com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.22.2")
      else Seq.empty
    ),
    Compile / mainClass := Some(bundleMainClass),
    Compile / packageBin / packageOptions += Package.ManifestAttributes(
      "Main-Class" -> bundleMainClass
    ),
    bundle := {
      val out = target.value / "bundle"
      IO.delete(out)
      IO.createDirectory(out)
      val app = (Compile / packageBin).value
      IO.copyFile(app, out / app.getName)
      (Runtime / fullClasspathAsJars).value
        .map(_.data)
        .filter(j => j.getName.endsWith(".jar") && j != app)
        .foreach(j => IO.copyFile(j, out / j.getName))
      out
    },
  )
