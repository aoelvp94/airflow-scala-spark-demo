"""Same bronze→silver transform as Pipeline.withPlain, in PySpark."""

from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

BRONZE_COLUMNS = [
    "OrderID",
    "Customer Name",
    "emailAddress",
    "AMOUNT_USD",
    "Order Date",
    "status",
    "Country Code",
    "Qty",
    "notes",
    "isVIP",
]
SILVER_COLUMNS = [
    "order_id",
    "customer_name",
    "email_address",
    "amount_usd",
    "order_date",
    "status",
    "country_code",
    "qty",
    "notes",
    "is_vip",
]
BRONZE_SCHEMA = StructType([StructField(c, StringType(), True) for c in BRONZE_COLUMNS])
TBILISI = timezone(timedelta(hours=4))


def to_snake(name: str) -> str:
    name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    name = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", name)
    name = re.sub(r"\s+", "_", name)
    return name.lower()


def with_plain(df):
    missing = [c for c in BRONZE_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"The [{', '.join(missing)}] columns are not included in the DataFrame "
            f"with the following columns [{', '.join(df.columns)}]"
        )
    renamed = df
    for name in df.columns:
        renamed = renamed.withColumnRenamed(name, to_snake(name))
    token = F.lower(F.trim(F.col("is_vip")))
    return (
        renamed.withColumn("customer_name", F.regexp_replace(F.trim(F.col("customer_name")), " +", " "))
        .withColumn("email_address", F.lower(F.trim(F.col("email_address"))))
        .withColumn("amount_usd", F.regexp_replace(F.trim(F.col("amount_usd")), ",", "").cast(DoubleType()))
        .withColumn(
            "order_date",
            F.coalesce(
                F.try_to_date(F.trim(F.col("order_date")), "yyyy-MM-dd"),
                F.try_to_date(F.trim(F.col("order_date")), "dd/MM/yyyy"),
            ),
        )
        .withColumn("status", F.lower(F.trim(F.col("status"))))
        .withColumn("country_code", F.upper(F.trim(F.col("country_code"))))
        .withColumn("qty", F.trim(F.col("qty")).cast(IntegerType()))
        .withColumn("notes", F.regexp_replace(F.trim(F.col("notes")), " +", " "))
        .withColumn(
            "is_vip",
            F.when(token.isin("y", "yes", "true", "1", "t"), F.lit(True)).otherwise(F.lit(False)),
        )
        .withColumn("order_id", F.trim(F.col("order_id")).cast(LongType()))
        .where(F.col("order_id").isNotNull())
        .dropDuplicates(["order_id"])
        .select(*SILVER_COLUMNS)
    )


def checksum(df) -> int:
    row = df.agg(
        F.sum(F.col("order_id")).alias("ids"),
        F.sum(F.coalesce(F.col("qty").cast(LongType()), F.lit(0))).alias("qty"),
        F.sum(F.coalesce(F.round(F.col("amount_usd") * 100, 0).cast(LongType()), F.lit(0))).alias("cents"),
    ).first()
    return int(row[0] + row[1] + row[2])


def stats(vals: list[int]) -> dict[str, int]:
    s = sorted(vals)
    mid = s[len(s) // 2] if len(s) % 2 == 1 else (s[len(s) // 2 - 1] + s[len(s) // 2]) // 2
    return {"median": mid, "min": s[0], "max": s[-1]}


def main() -> None:
    bronze_path = os.environ.get("PIPELINE_BRONZE", "/tmp/pipeline/bench/bronze")
    silver_path = os.environ.get("PIPELINE_SILVER", "/tmp/pipeline/bench/silver-pyspark")
    out = Path(os.environ.get("PIPELINE_BENCH_OUT", "bench/pipeline-pyspark.json"))
    runs = int(os.environ.get("BENCH_RUNS", "7"))
    warmup = int(os.environ.get("BENCH_WARMUP", "1"))
    expected_checksum = os.environ.get("PIPELINE_CHECKSUM")
    expected_rows = os.environ.get("PIPELINE_SILVER_ROWS")

    t0 = time.perf_counter_ns()
    spark = (
        SparkSession.builder.appName("pipeline-pyspark")
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    spark_start_ms = (time.perf_counter_ns() - t0) // 1_000_000
    bronze = spark.read.schema(BRONZE_SCHEMA).parquet(bronze_path)
    bronze_rows = bronze.count()
    warm: list[dict] = []
    measured: list[dict] = []
    try:
        for i in range(warmup + runs):
            tag = "warmup" if i < warmup else "bench"
            t1 = time.perf_counter_ns()
            silver = with_plain(bronze)
            silver.write.mode("overwrite").parquet(silver_path)
            written = spark.read.parquet(silver_path)
            n = written.count()
            chk = checksum(written)
            compute_ms = (time.perf_counter_ns() - t1) // 1_000_000
            rec = {
                "mode": "pyspark",
                "run_id": f"{tag}-{i}",
                "state": "success",
                "bronze_rows": bronze_rows,
                "silver_rows": n,
                "checksum": chk,
                "compute_ms": compute_ms,
            }
            (warm if i < warmup else measured).append(rec)
            print(json.dumps(rec, sort_keys=True))
    finally:
        spark.stop()

    checksums = {r["checksum"] for r in measured}
    rows = {r["silver_rows"] for r in measured}
    if len(checksums) != 1 or len(rows) != 1:
        raise SystemExit(f"pyspark runs disagreed: checksums={checksums} rows={rows}")
    if expected_checksum and str(next(iter(checksums))) != str(expected_checksum):
        raise SystemExit(f"checksum {checksums} != expected {expected_checksum}")
    if expected_rows and str(next(iter(rows))) != str(expected_rows):
        raise SystemExit(f"silver_rows {rows} != expected {expected_rows}")

    doc = {
        "generated_at": datetime.now(TBILISI).strftime("%Y-%m-%dT%H:%M:%S%z"),
        "spark": "4.3.0 local[*] pyspark",
        "bronze_path": bronze_path,
        "bronze_rows": bronze_rows,
        "spark_session_start_ms": spark_start_ms,
        "runs_per_mode": runs,
        "warmup_per_mode": warmup,
        "summary": {
            "pyspark": {
                "n": len(measured),
                "compute_ms": stats([r["compute_ms"] for r in measured]),
                "bronze_rows": bronze_rows,
                "silver_rows": next(iter(rows)),
                "checksum": next(iter(checksums)),
            }
        },
        "runs": measured,
        "warmup": warm,
    }
    out = out.resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(doc, indent=2) + "\n")
    print(f"wrote {out}")
    print(json.dumps(doc["summary"], indent=2))


if __name__ == "__main__":
    main()
