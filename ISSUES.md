# Issue drafts (not posted)

All drafts are in English and ready to paste. Nothing here has been filed. Drafts 1–3 target apache/airflow (bug report / feature request forms); draft 4 targets astronomer/astro-cli. Every finding was reproduced locally on 2026-11-16 with Airflow 3.4.0, the Java SDK 1.0.0 from Maven Central, Scala 2.13.18 and Spark 4.3.0 (plus Spark 3.5.9 where stated).

---

## 1. apache/airflow (bug report, category "Task SDK")

**Title:** Java SDK: Spark bundles built with sbt or Maven fail with a Jackson version mismatch because airflow-sdk 1.0.0 pulls jackson-databind 2.22

**Apache Airflow version:** 3.4.0 (Java SDK `org.apache.airflow:airflow-sdk:1.0.0`)

**What happened and how to reproduce it?**

`airflow-sdk:1.0.0` depends on `jackson-databind:2.22.2`. Spark depends on `jackson-module-scala`, which checks at runtime that jackson-databind is on the same minor version. Gradle aligns the two on its own: Jackson publishes Gradle module metadata with a `jackson-bom` platform constraint, so the `scala_spark_example` resolves `jackson-module-scala_2.13:2.15.2 -> 2.22.2`. sbt and Maven do not do this alignment. They keep Spark's `jackson-module-scala`, upgrade only `jackson-databind`, and the first Spark action then fails inside the task JVM:

```
Caused by: com.fasterxml.jackson.databind.JsonMappingException: Scala module 2.21.2 requires Jackson Databind version >= 2.21.0 and < 2.22.0 - Found jackson-databind version 2.22.2
	at com.fasterxml.jackson.module.scala.JacksonModule.setupModule(JacksonModule.scala:62)
	at org.apache.spark.rdd.RDDOperationScope$.<clinit>(RDDOperationScope.scala:82)
```

Resolved versions per build tool and Spark version:

| Build | Spark | jackson-databind | jackson-module-scala | Result |
|---|---|---|---|---|
| sbt 1.13.1 | 3.5.9 | 2.22.2 | 2.15.2 | `Scala module 2.15.2 requires Jackson Databind version >= 2.15.0 and < 2.16.0` |
| sbt 1.13.1 | 4.3.0 | 2.22.2 | 2.21.2 | `Scala module 2.21.2 requires Jackson Databind version >= 2.21.0 and < 2.22.0` |
| Gradle 8.14 (`java-sdk/scala_spark_example/build.gradle`) | 3.5.9 | 2.22.2 | 2.22.2 (aligned via `jackson-bom`) | resolves consistently |

Minimal sbt build to reproduce:

```scala
scalaVersion := "2.13.18"
libraryDependencies ++= Seq(
  "org.apache.airflow" % "airflow-sdk" % "1.0.0",
  "org.apache.spark" %% "spark-sql" % "4.3.0"
)
```

Any task that calls `SparkSession.builder().master("local[*]").getOrCreate()` and then runs `spark.range(10).count()` fails with the exception above. The task is marked `failed` and the cause appears in the task log.

**What you think should happen instead?**

A JVM bundle that adds Spark (or any other library that pins Jackson) should not have to reconcile the SDK's Jackson version with its own. Options, roughly in order of preference:

1. Relocate the SDK's internal dependencies (jackson-core, jackson-databind and the msgpack, YAML and JSR-310 modules, plus Kotlin coroutines and serialization, and ktor) inside `airflow-sdk`, or publish an `airflow-sdk-shaded` artifact, so user code never sees them on the classpath. The SDK already keeps Jackson off the user's compile classpath (`implementation` scope). The one user-facing contact point is `DagRun.getConf(type)` honoring `@JsonProperty`, which keeps working if `jackson-annotations` stays unrelocated. Its annotations are stable across 2.x.
2. If relocation is not wanted, document the conflict in the "Building a bundle" section of the Java SDK guide and in `java-sdk/scala_spark_example/README.md`. Show the workaround for sbt and Maven: align `jackson-module-scala` to the SDK's Jackson minor (`dependencyOverrides += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.22.2"` in sbt, or import `com.fasterxml.jackson:jackson-bom:2.22.2` in Maven `dependencyManagement`). Also note that Gradle only works because of Jackson's platform alignment.

**Operating System:** Debian (apache/airflow:3.4.0-python3.12 image), Temurin 17 JRE

**Deployment:** Astronomer (Astro CLI 1.47.0 `astro dev start` on `apache/airflow:3.4.0-python3.12`)

**Anything else?**

The same root cause shows up as a Log4j skew: sbt resolves `log4j-api:2.26.1` (from `airflow-sdk-log4j2`) next to Spark's `log4j-core:2.25.4`. The Gradle example avoids it with `platform("org.apache.logging.log4j:log4j-bom:2.26.1")`, but sbt and Maven users need to align it by hand.

**Are you willing to submit PR?** Yes

Generated-by: Grok Bot following [the guidelines](https://github.com/apache/airflow/blob/main/contributing-docs/05_pull_requests.rst#gen-ai-assisted-contributions)

---

## 2. apache/airflow (bug report, category "Task SDK")

**Title:** Java SDK: `Client.setXCom` silently stores Scala collections, `Option` and case classes as bean properties instead of rejecting them

**Apache Airflow version:** 3.4.0 (Java SDK `org.apache.airflow:airflow-sdk:1.0.0`)

**What happened and how to reproduce it?**

From Scala, calling `client.setXCom(value)` with a Scala value succeeds. The task is marked `success`, but the stored XCom is Jackson's bean introspection of the object, not its data:

| Value pushed from Scala | XCom stored (read back through the REST API) |
|---|---|
| `Map("a" -> 1, "b" -> 2)` | `{"empty": false, "traversableAgain": true, "scala$collection$immutable$Map$Map2$$key1": "a", "scala$collection$immutable$Map$Map2$$key2": "b", "scala$collection$immutable$Map$Map2$$value1": 1, "scala$collection$immutable$Map$Map2$$value2": 2}` |
| `Seq(1, 2, 3)` | `{"empty": false, "traversableAgain": true}` |
| `Some(1L)` | `{"empty": false, "defined": true}` |
| `case class Point(x: Int, y: Int)`, `Point(1, 2)` | `{}` |
| `BigDecimal("1.5")` (scala.math) | `1.5` as a double |
| `Map[String, Any]("a" -> 1, "nested" -> Seq(1, 2).asJava).asJava` | `{"a": 1, "nested": [1, 2]}` (correct) |

Reproducer: one `Task` per row, each calling `client.setXCom(...)` with the value above. A downstream task reads them back with `client.getXCom(taskId)`. Every push task succeeds.

The cause is in `Frame.toBody`: the whole `SetXCom` message goes through `mapper.convertValue(value, MutableMap::class.java)`. That turns any object into a map of its bean properties before `MessagePacker.packAny` runs, so the strict type check in `packAny` (`Unsupported data type`) never sees the original value. Every request is encoded this way, so `TaskStateStore.set` is affected too.

**What you think should happen instead?**

A value that is not a JSON value should fail the task at `setXCom` with a clear message, the same way byte arrays already do. One way is to run the `packAny` type rules on the XCom value before the message is converted. The error should say which types are accepted: `null`, `Boolean`, numbers, `String`, `java.util.Map` with `String` keys, and `java.util.Collection`.

Optionally, call `ObjectMapper.findAndRegisterModules()` (or an opt-in equivalent) so that a `jackson-module-scala` already on the classpath serializes Scala collections, `Option` and case classes properly. Spark bundles always carry that module.

The Java SDK guide could also add a short note for Kotlin and Scala users: convert to `java.util` collections (`.asJava`) before pushing.

**Operating System:** Debian (apache/airflow:3.4.0-python3.12 image), Temurin 17 JRE

**Deployment:** Astronomer (Astro CLI 1.47.0 `astro dev start` on `apache/airflow:3.4.0-python3.12`)

**Are you willing to submit PR?** Yes

Generated-by: Grok Bot following [the guidelines](https://github.com/apache/airflow/blob/main/contributing-docs/05_pull_requests.rst#gen-ai-assisted-contributions)

---

## 3. apache/airflow (feature request)

**Title:** Java SDK guide: document building a bundle with sbt (Scala) next to Gradle and Maven

**Description**

The Java SDK guide says Dags and tasks can be written in Scala, but "Building a bundle" only covers Gradle and Maven, and `java-sdk/scala_spark_example` uses Gradle. A Scala project built with sbt works with JavaCoordinator: a Python `@task.stub` Dag, a native Dag declared with `DagDef` from Scala, and Spark 4.3.0 `local[*]` inside the task JVM all ran end to end on Airflow 3.4.0. Getting there took several steps that are not documented:

1. **Thin bundle.** Set `Main-Class` with `Package.ManifestAttributes("Main-Class" -> "...")` on `Compile / packageBin`, then copy `Runtime / fullClasspathAsJars` next to the app jar. `Airflow-Supervisor-Schema-Version` then comes from the `airflow-sdk` jar as-is.
2. **Fat jar with sbt-assembly.** sbt-assembly writes its own manifest without `Airflow-Supervisor-Schema-Version`, so JavaCoordinator fails with `FileNotFoundError: cannot find a JAR with Airflow-Supervisor-Schema-Version metadata`. The guide should show `assembly / packageOptions += Package.ManifestAttributes("Airflow-Supervisor-Schema-Version" -> "2026-10-30")`. It should also warn that `META-INF/org/apache/logging/log4j/core/config/plugins/Log4j2Plugins.dat` must be merged rather than taken from the first jar, or `AirflowAppender` and Log4j's own plugins can go missing.
3. **Main-Class discovery.** A Spark thin bundle carries six dependency jars that set `Main-Class`: `commons-compress`, `commons-crypto`, `compress-lzf`, `icu4j`, `ivy` and `javassist`. Without `main_class`, `_JarInfo.find` picks `org.apache.commons.compress.archivers.Lister`. The guide already says to set `main_class`; an sbt example should set it too.
4. **Annotation style.** The `@Airflow.Dag` / `@Airflow.Deps` style relies on a javac annotation processor, which scalac does not run. Scala users should use the `DagDef` interface style and `Bundle.register(dagId, taskId, classOf[...])`.
5. **Scala collections in `DagDef.config`.** `config("tags", ...)` takes a `java.lang.Iterable` of `String`, so pass `Seq(...).asJava`.
6. **Dag source in the Code view.** sbt jars carry no `META-INF/airflow/sources.json`, so the Code view shows "This JAR embeds no Dag source". The Maven recipe for packing sources (resource mapping, `sources.json` and the `Airflow-Java-SDK-Sources` manifest entry) translates to sbt as a `Compile / resourceGenerators` task. It would help to show it.
7. **Jackson and Log4j alignment** for Spark bundles: see the related bug report on the Jackson mismatch.

**Use case/motivation**

Spark jobs are mostly written in Scala and built with sbt. With these steps documented, a Spark team can move an existing sbt project onto JavaCoordinator without switching build tools.

**Related issues**

#71134 (ambiguous entrypoints when `main_class` is unset); the Jackson-mismatch bug report filed alongside this one.

**Are you willing to submit a PR?** Yes

Generated-by: Grok Bot following [the guidelines](https://github.com/apache/airflow/blob/main/contributing-docs/05_pull_requests.rst#gen-ai-assisted-contributions)

---

## 4. astronomer/astro-cli

**Title:** `astro dev start` cannot run an `apache/airflow:3.4.0` base image, which is the only way to try the Airflow 3.4 language SDKs locally today

**Describe the bug**

Airflow 3.4.0 ships the Java SDK coordinator (`airflow.sdk.coordinators.java`) and native Dag parsing for JVM bundles. The newest Astro Runtime on quay.io is `3.3-12`, which is based on Airflow 3.3. The Task SDK 1.3.x line has no Java Dag importer (`airflow/sdk/coordinators/java/_dag_importer.py` is first in 1.4.0), and the Java SDK 1.0.0 needs supervisor schema `2026-10-30`, which is Airflow 3.4.0 or later. `astro dev init` writes `FROM astrocrpublic.azurecr.io/runtime:3.3-12`. To try the Java SDK, the Dockerfile has to use `FROM apache/airflow:3.4.0-python3.12`, and `astro dev start` (Astro CLI 1.47.0) then fails three times in a row:

1. `Error: error creating docker-compose project: failed to create project: runtime version label not found`. Worked around with `LABEL io.astronomer.docker.runtime.version="3.3-12"` in the Dockerfile.
2. `unable to find user astro: no matching entries in passwd file`, because the generated compose file runs every Airflow service as `user: astro`. Worked around with `echo "astro:x:50000:0:astro:/home/airflow:/bin/bash" >> /etc/passwd`.
3. The dag-processor crash-loops with `FileNotFoundError: [Errno 2] No such file or directory: '/usr/local/airflow/logs/dag_processor/...'`, because the `airflow_logs` named volume is created root-owned when the image has no `/usr/local/airflow/logs` owned by the Airflow user. Worked around by creating that directory owned by `airflow` in the Dockerfile and recreating the volume.

With the three workarounds and `ENV AIRFLOW_HOME=/usr/local/airflow`, the project runs: a Scala/Spark bundle served by JavaCoordinator, a native Scala Dag, and Python Dags all parse and run.

**What CLI command did you run?**

```
astro dev init
astro dev start --no-browser --wait 3m
```

**What Astro CLI version are you using?** 1.47.0

**What Operating System are you using?** Linux (Debian-based VM), Docker 29.1.4, Compose v2.32.4

**Expected behavior**

Either `astro dev start` accepts a plain `apache/airflow` 3.x image (falling back to the `org.apache.airflow` labels or the Airflow version inside the image, not running services as `astro` when that user does not exist, and creating the logs directory with the right owner), or an Astro Runtime image based on Airflow 3.4 is published. Ideally that image would offer a JRE variant or document the supported way to add one. The Java SDK needs Java 11+, and Spark 3.5 needs Java 17 or lower.

**Additional context**

The project also needed a `docker-compose.override.yml` with `!reset` keys for host networking. That part is specific to the test host, whose Docker bridge blocks traffic between containers, and is not part of this report.
