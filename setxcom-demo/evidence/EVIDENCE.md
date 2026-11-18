# Reviewer demo evidence

Zone: Asia/Tbilisi (UTC+4). Timestamps below are converted from UTC.
These runs were produced by the README steps in this directory (local only; nothing was published).

## baseline

- dag_id: `setxcom_fix`
- run_id: `reviewer_demo_baseline_20261118T201448`
- start: 2026-11-18 20:14:53 Asia/Tbilisi (UTC+4)
- end: 2026-11-18 20:15:00 Asia/Tbilisi (UTC+4)
- DAG state: **success**

| task | state | XCom `return_value` | error |
|---|---|---|---|
| `extract` | success | `42` |  |
| `score` | success | `{"region": "eu", "threshold": 0.5}` |  |
| `push_as_java` | success | `{"region": "eu", "threshold": 0.5}` |  |
| `push_java_pojo` | success | `{"region": "eu", "threshold": 0.5}` |  |
| `push_scala_map` | success | `{"scala$collection$immutable$Map$Map2$$key1": "region", "scala$collection$immutable$Map$Map2$$value1": "eu", "scala$collection$immutable$Map$Map2$$key2": "threshold", "scala$collection$immutable$Map$Map2$$value2": 0.5, "empty": false, "traversableAgain": true}` |  |
| `push_scala_seq` | success | `{"empty": false, "traversableAgain": true}` |  |
| `push_scala_option` | success | `{"empty": false, "defined": true}` |  |
| `push_case_class` | success | `{}` |  |

## patched

- dag_id: `setxcom_fix`
- run_id: `reviewer_demo_patched_20261118T201520`
- start: 2026-11-18 20:15:25 Asia/Tbilisi (UTC+4)
- end: 2026-11-18 20:15:30 Asia/Tbilisi (UTC+4)
- DAG state: **failed**

| task | state | XCom `return_value` | error |
|---|---|---|---|
| `extract` | success | `42` |  |
| `score` | success | `{"region": "eu", "threshold": 0.5}` |  |
| `push_as_java` | success | `{"region": "eu", "threshold": 0.5}` |  |
| `push_java_pojo` | success | `{"region": "eu", "threshold": 0.5}` |  |
| `push_scala_map` | failed | _(none written)_ | `java.lang.IllegalArgumentException: Unsupported XCom value type: scala.collection.immutable.Map$Map2. Scala collections, Option and case classes cannot be stored as XComs directly. Convert a Scala collection with scala.jdk.CollectionConverters (.asJava), and convert a case class or Option to a java.util.Map or list.` |
| `push_scala_seq` | failed | _(none written)_ | `java.lang.IllegalArgumentException: Unsupported XCom value type: scala.collection.immutable.$colon$colon. Scala collections, Option and case classes cannot be stored as XComs directly. Convert a Scala collection with scala.jdk.CollectionConverters (.asJava), and convert a case class or Option to a java.util.Map or list.` |
| `push_scala_option` | failed | _(none written)_ | `java.lang.IllegalArgumentException: Unsupported XCom value type: scala.Some. Scala collections, Option and case classes cannot be stored as XComs directly. Convert a Scala collection with scala.jdk.CollectionConverters (.asJava), and convert a case class or Option to a java.util.Map or list.` |
| `push_case_class` | failed | _(none written)_ | `java.lang.IllegalArgumentException: Unsupported XCom value type: com.mycompany.airflow.hello.ScoreSummary. Scala collections, Option and case classes cannot be stored as XComs directly. Convert a Scala collection with scala.jdk.CollectionConverters (.asJava), and convert a case class or Option to a java.util.Map or list.` |

