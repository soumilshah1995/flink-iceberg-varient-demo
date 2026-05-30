#!/usr/bin/env python3
"""
Read Flink-written Iceberg V3 table from local MinIO via REST catalog and query VARIANT.

Prerequisites:
  docker compose up -d
  ./scripts/pipeline.sh start
  docker exec python python /workspace/producer.py

Run locally:

```bash
cd /Users/sshah/IdeaProjects/study-learn/flink
export PACKAGES="org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0"
python3 simple_fanout_test.py
```
"""

from __future__ import annotations

import os
import sys

# --- Spark / Iceberg (before pyspark import) ---
os.environ.setdefault("JAVA_HOME", "/opt/homebrew/opt/openjdk@17")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

DEFAULT_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0,"
    "org.apache.iceberg:iceberg-aws-bundle:1.11.0"
)
PACKAGES = os.environ.get("PACKAGES", DEFAULT_PACKAGES)
os.environ["PYSPARK_SUBMIT_ARGS"] = f"--packages {PACKAGES} pyspark-shell"

CATALOG = "iceberg_rest"
TABLE = f"{CATALOG}.demo.orders_variant"

# MinIO + Iceberg REST (same as docker-compose / Flink pipeline)
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "password")
ICEBERG_REST_URI = os.environ.get("ICEBERG_REST_URI", "http://localhost:8181")
WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE", "s3://warehouse/")

from pyspark.sql import SparkSession


def create_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("IcebergVariantQuery")
        .master("local[*]")
        .config("spark.jars.packages", PACKAGES)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
        .config(f"spark.sql.catalog.{CATALOG}.uri", ICEBERG_REST_URI)
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
        .config(
            f"spark.sql.catalog.{CATALOG}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", MINIO_ENDPOINT)
        .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{CATALOG}.client.region", "us-east-1")
        .config(f"spark.sql.catalog.{CATALOG}.s3.access-key-id", MINIO_ACCESS_KEY)
        .config(
            f"spark.sql.catalog.{CATALOG}.s3.secret-access-key",
            MINIO_SECRET_KEY,
        )
        .config("spark.sql.defaultCatalog", CATALOG)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def show_table_schema(spark: SparkSession) -> None:
    print(f"\n=== schema: {TABLE} ===")
    spark.table(TABLE).printSchema()
    print(f"row count: {spark.table(TABLE).count()}")


def query_all_columns(spark: SparkSession) -> None:
    print("\n=== all columns (limit 10) ===")
    spark.sql(
        f"""
        SELECT
          order_id,
          site_id,
          product_name,
          order_value,
          priority,
          order_date,
          ts,
          event_variant
        FROM {TABLE}
        ORDER BY order_id
        LIMIT 10
        """
    ).show(truncate=40)


def query_schema_version(spark: SparkSession) -> None:
    """Print event_variant.schema_version (nested VARIANT field)."""
    print("\n=== event_variant.schema_version ===")
    spark.sql(
        f"""
        SELECT
          order_id,
          site_id,
          variant_get(event_variant, '$.schema_version', 'int') AS event_variant_schema_version
        FROM {TABLE}
        ORDER BY order_id
        LIMIT 10
        """
    ).show(truncate=30)


def query_variant_nested(spark: SparkSession) -> None:
    """Spark can read nested paths from stored VARIANT via variant_get."""
    print("\n=== nested VARIANT fields (variant_get) ===")
    spark.sql(
        f"""
        SELECT
          order_id,
          site_id,
          variant_kind,
          variant_user_id,
          variant_theme,
          variant_get(event_variant, '$.schema_version', 'int') AS v_schema_version,
          variant_get(event_variant, '$.kind', 'string') AS v_kind,
          variant_get(event_variant, '$.user.id', 'string') AS v_user_id,
          variant_get(event_variant, '$.user.prefs.theme', 'string') AS v_theme,
          variant_get(event_variant, '$.scores.k0', 'int') AS v_score_k0,
          variant_get(event_variant, '$.line_items[0].sku', 'string') AS v_first_sku,
          try_variant_get(event_variant, '$.note', 'string') AS v_note
        FROM {TABLE}
        ORDER BY order_id
        LIMIT 10
        """
    ).show(truncate=30)


def query_variant_filter(spark: SparkSession) -> None:
    print("\n=== filter on nested VARIANT ===")
    spark.sql(
        f"""
        SELECT order_id, site_id, variant_kind, variant_theme
        FROM {TABLE}
        WHERE variant_get(event_variant, '$.kind', 'string') = 'order'
          AND variant_get(event_variant, '$.user.prefs.theme', 'string') = 'light'
        LIMIT 5
        """
    ).show(truncate=30)


def main() -> None:
    spark = create_spark()
    print("Spark", spark.version)
    print(f"REST catalog: {ICEBERG_REST_URI}")
    print(f"MinIO: {MINIO_ENDPOINT}  warehouse: {WAREHOUSE}")

    show_table_schema(spark)
    query_schema_version(spark)
    query_all_columns(spark)
    query_variant_nested(spark)
    query_variant_filter(spark)

    spark.stop()
    print("\nDone.")


if __name__ == "__main__":
    main()
