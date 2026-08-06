# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver: parse, validate, deduplicate
# MAGIC
# MAGIC Bronze holds raw JSON strings. Silver applies an **explicit schema** (never
# MAGIC `inferSchema` on a stream — inference needs a full pass over data that has not arrived
# MAGIC yet), enforces quality rules, and removes duplicates that at-least-once delivery
# MAGIC inevitably produces.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, IntegerType,
)

LAKE = "abfss://lakehouse@yourstorageaccount.dfs.core.windows.net"
BRONZE_PATH = f"{LAKE}/bronze/clickstream"
SILVER_PATH = f"{LAKE}/silver/clickstream"
QUARANTINE_PATH = f"{LAKE}/silver/clickstream_quarantine"
SILVER_CHECKPOINT = f"{LAKE}/_checkpoints/silver_clickstream"
QUARANTINE_CHECKPOINT = f"{LAKE}/_checkpoints/silver_quarantine"

WATERMARK_DELAY = "10 minutes"

# COMMAND ----------

event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("referrer", StringType(), True),
    StructField("country", StringType(), True),
    StructField("device", StringType(), True),
    StructField("duration_ms", IntegerType(), True),
    StructField("event_time", TimestampType(), False),
])

# COMMAND ----------

bronze_stream = spark.readStream.format("delta").load(BRONZE_PATH)

parsed = bronze_stream.select(
    F.from_json(F.col("raw_payload"), event_schema).alias("e"),
    F.col("source_partition"),
    F.col("source_offset"),
    F.col("ingested_at"),
).select("e.*", "source_partition", "source_offset", "ingested_at")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality rules
# MAGIC
# MAGIC Rows are flagged rather than silently dropped. Valid rows go to Silver, invalid rows go
# MAGIC to a quarantine table — the failure reason is data you want when a source starts
# MAGIC misbehaving at 3am.

# COMMAND ----------

VALID_EVENT_TYPES = ["page_view", "click", "scroll", "add_to_cart", "purchase"]

flagged = parsed.withColumn(
    "dq_failure_reason",
    F.when(F.col("event_id").isNull(), F.lit("missing_event_id"))
    .when(F.col("event_time").isNull(), F.lit("missing_event_time"))
    .when(~F.col("event_type").isin(VALID_EVENT_TYPES), F.lit("unknown_event_type"))
    .when(F.col("duration_ms") < 0, F.lit("negative_duration"))
    .otherwise(F.lit(None)),
)

valid = flagged.filter(F.col("dq_failure_reason").isNull()).drop("dq_failure_reason")
invalid = flagged.filter(F.col("dq_failure_reason").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Watermark + deduplication
# MAGIC
# MAGIC `dropDuplicatesWithinWatermark` (DBR 14.0+) keeps dedupe state bounded to the watermark
# MAGIC window instead of growing forever. On older runtimes use
# MAGIC `.withWatermark(...).dropDuplicates(["event_id", "event_time"])`.

# COMMAND ----------

deduped = (
    valid
    .withWatermark("event_time", WATERMARK_DELAY)
    .dropDuplicatesWithinWatermark(["event_id"])
    .withColumn("event_date", F.to_date("event_time"))
    .withColumn("page_path", F.regexp_extract("page_url", r"https?://[^/]+(/[^?]*)", 1))
    .withColumn("country", F.upper(F.coalesce(F.col("country"), F.lit("UNKNOWN"))))
    .withColumn("processed_at", F.current_timestamp())
)

# COMMAND ----------

(
    deduped.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", SILVER_CHECKPOINT)
    .option("mergeSchema", "true")
    .partitionBy("event_date")
    .trigger(availableNow=True)
    .start(SILVER_PATH)
)

(
    invalid.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", QUARANTINE_CHECKPOINT)
    .trigger(availableNow=True)
    .start(QUARANTINE_PATH)
)

for q in spark.streams.active:
    q.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Maintenance
# MAGIC
# MAGIC Streaming writes produce many small files. Compact them on a schedule, not every batch.

# COMMAND ----------

spark.sql(f"OPTIMIZE delta.`{SILVER_PATH}` ZORDER BY (country, event_type)")
spark.sql(f"VACUUM delta.`{SILVER_PATH}` RETAIN 168 HOURS")
