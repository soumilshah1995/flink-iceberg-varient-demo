-- Flink SQL: Kafka (JSON) -> Iceberg V3 table
-- event_variant stored as VARIANT with Parquet shredding enabled (shred-variants=true).
-- Nested paths also extracted at ingest (Flink 2.1 cannot query stored VARIANT on read).

SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10 s';
SET 'parallelism.default' = '1';
SET 'pipeline.name' = 'kafka-iceberg-variant-sql';

CREATE TEMPORARY TABLE kafka_raw (
  `value` BYTES
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders_variant',
  'properties.bootstrap.servers' = 'broker:9092',
  'properties.group.id' = 'flink-sql-iceberg-variant',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'raw'
);

CREATE CATALOG IF NOT EXISTS iceberg_rest WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://iceberg-rest:8181',
  'warehouse' = 's3://warehouse/',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://minio:9000',
  's3.path-style-access' = 'true',
  'client.region' = 'us-east-1',
  's3.access-key-id' = 'admin',
  's3.secret-access-key' = 'password'
);

USE CATALOG iceberg_rest;
CREATE DATABASE IF NOT EXISTS demo;
USE demo;

CREATE TABLE orders_variant (
  order_id STRING,
  site_id STRING,
  product_name STRING,
  order_value STRING,
  priority STRING,
  order_date STRING,
  ts STRING,
  event_variant VARIANT,
  variant_kind STRING,
  variant_user_id STRING,
  variant_theme STRING,
  variant_score_k0 INT,
  variant_first_sku STRING,
  variant_note STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'format-version' = '3',
  'write.format.default' = 'parquet',
  'shred-variants' = 'true',
  'variant-inference-buffer-size' = '100'
);

INSERT INTO orders_variant
SELECT
  JSON_VALUE(payload, '$.order_id' RETURNING STRING),
  JSON_VALUE(payload, '$.site_id' RETURNING STRING),
  JSON_VALUE(payload, '$.product_name' RETURNING STRING),
  JSON_VALUE(payload, '$.order_value' RETURNING STRING),
  JSON_VALUE(payload, '$.priority' RETURNING STRING),
  JSON_VALUE(payload, '$.order_date' RETURNING STRING),
  JSON_VALUE(payload, '$.ts' RETURNING STRING),
  PARSE_JSON(JSON_QUERY(payload, '$.event_variant' RETURNING STRING)),
  JSON_VALUE(payload, '$.event_variant.kind' RETURNING STRING),
  JSON_VALUE(payload, '$.event_variant.user.id' RETURNING STRING),
  JSON_VALUE(payload, '$.event_variant.user.prefs.theme' RETURNING STRING),
  JSON_VALUE(payload, '$.event_variant.scores.k0' RETURNING INT),
  JSON_VALUE(payload, '$.event_variant.line_items[0].sku' RETURNING STRING),
  JSON_VALUE(payload, '$.event_variant.note' RETURNING STRING)
FROM (
  SELECT CAST(`value` AS STRING) AS payload
  FROM default_catalog.default_database.kafka_raw
);
