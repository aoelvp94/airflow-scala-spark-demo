# Reviewer demo evidence

Zone: Asia/Tbilisi (UTC+4). Timestamps below are converted from UTC.
These runs were produced by the README steps in this directory (local only; nothing was published).

## baseline

- dag_id: `setxcom_fix`
- run_id: `reviewer_demo_baseline_20261127T190512`
- start: 2026-11-27 19:05:16 Asia/Tbilisi (UTC+4)
- end: 2026-11-27 19:06:16 Asia/Tbilisi (UTC+4)
- DAG state: **failed**

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
| `push_empty_object` | failed | _(none written)_ | `java.lang.IllegalArgumentException: No serializer found for class com.mycompany.airflow.hello.EmptyMarker and no properties discovered to create BeanSerializer (to avoid exception, disable SerializationFeature.FAIL_ON_EMPTY_BEANS) (through reference chain: org.apache.airflow.sdk.execution.comm.SetXCom["value"])` |

## patched

- dag_id: `setxcom_fix`
- run_id: `reviewer_demo_patched_20261127T190642`
- start: 2026-11-27 19:06:47 Asia/Tbilisi (UTC+4)
- end: 2026-11-27 19:07:39 Asia/Tbilisi (UTC+4)
- DAG state: **failed**

| task | state | XCom `return_value` | error |
|---|---|---|---|
| `extract` | success | `42` |  |
| `score` | success | `{"region": "eu", "threshold": 0.5}` |  |
| `push_as_java` | success | `{"region": "eu", "threshold": 0.5}` |  |
| `push_java_pojo` | success | `{"region": "eu", "threshold": 0.5}` |  |
| `push_scala_map` | success | `{"scala$collection$immutable$Map$Map2$$key1": "region", "scala$collection$immutable$Map$Map2$$value1": "eu", "scala$collection$immutable$Map$Map2$$key2": "threshold", "scala$collection$immutable$Map$Map2$$value2": 0.5, "empty": false, "traversableAgain": true}` |  |
| `push_scala_seq` | success | `{"empty": false, "traversableAgain": true}` |  |
| `push_scala_option` | success | `{"empty": false, "defined": true}` |  |
| `push_case_class` | failed | _(none written)_ | `java.lang.IllegalArgumentException: Cannot send com.mycompany.airflow.hello.ScoreSummary to Airflow: Jackson finds no properties to encode, so it would be stored as {}. Convert it to a java.util.Map, a java.util.List or a JSON scalar, or add a Jackson module for the type to the bundle (jackson-module-scala for Scala collections, Option and case classes).` |
| `push_empty_object` | failed | _(none written)_ | `java.lang.IllegalArgumentException: Cannot send com.mycompany.airflow.hello.EmptyMarker to Airflow: Jackson finds no properties to encode, so it would be stored as {}. Convert it to a java.util.Map, a java.util.List or a JSON scalar, or add a Jackson module for the type to the bundle (jackson-module-scala for Scala collections, Option and case classes).` |

## patched-scala

- dag_id: `setxcom_fix`
- run_id: `reviewer_demo_patched_scala_20261127T190810`
- start: 2026-11-27 19:08:18 Asia/Tbilisi (UTC+4)
- end: 2026-11-27 19:09:58 Asia/Tbilisi (UTC+4)
- DAG state: **failed**

| task | state | XCom `return_value` | error |
|---|---|---|---|
| `extract` | success | `42` |  |
| `score` | success | `{"region": "eu", "threshold": 0.5}` |  |
| `push_as_java` | success | `{"region": "eu", "threshold": 0.5}` |  |
| `push_java_pojo` | success | `{"region": "eu", "threshold": 0.5}` |  |
| `push_scala_map` | success | `{"region": "eu", "threshold": 0.5}` |  |
| `push_scala_seq` | success | `[1, 2, 3]` |  |
| `push_scala_option` | success | `"eu"` |  |
| `push_case_class` | success | `{"region": "eu", "threshold": 0.5}` |  |
| `push_empty_object` | failed | _(none written)_ | `java.lang.IllegalArgumentException: Cannot send com.mycompany.airflow.hello.EmptyMarker to Airflow: Jackson finds no properties to encode, so it would be stored as {}. Convert it to a java.util.Map, a java.util.List or a JSON scalar, or add a Jackson module for the type to the bundle (jackson-module-scala for Scala collections, Option and case classes).` |

