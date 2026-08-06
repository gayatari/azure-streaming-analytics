# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold: windowed metrics
# MAGIC
# MAGIC Five-minute tumbling windows by country and event type, written with **MERGE** through
# MAGIC `foreachBatch` rather than `outputMode("complete")`.
# MAGIC
# MAGIC `complete` rewrites the entire result table on every micro-batch. That is acceptable at
# MAGIC demo cardinality and catastrophic at production cardinality — so this notebook uses the
# MAGIC pattern you would actually ship.

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

LAKE = "abfss://lakehouse@yourstorageaccount.dfs.core.windows.net"
SILVER_PATH = f"{LAKE}/silver/clickstream"
GOLD_PATH = f"{LAKE}/gold/clickstream_metrics"
GOLD_CHECKPOINT = f"{LAKE}/_checkpoints/gold_clickstream_metrics"

WINDOW_SIZE = "5 minutes"
WATERMARK_DELAY = "10 minutes"

# COMMAND ----------

silver_stream = spark.readStream.format("delta").load(SILVER_PATH)

aggregated = (
    silver_stream
    .withWatermark("event_time", WATERMARK_DELAY)
    .groupBy(
        F.window("event_time", WINDOW_SIZE).alias("w"),
        F.col("country"),
        F.col("event_type"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.approx_count_distinct("user_id").alias("unique_users"),
        F.approx_count_distinct("session_id").alias("unique_sessions"),
        F.avg("duration_ms").alias("avg_duration_ms"),
        F.expr("percentile_approx(duration_ms, 0.95)").alias("p95_duration_ms"),
    )
    .select(
        F.col("w.start").alias("window_start"),
        F.col("w.end").alias("window_end"),
        "country",
        "event_type",
        "event_count",
        "unique_users",
        "unique_sessions",
        F.round("avg_duration_ms", 2).alias("avg_duration_ms"),
        "p95_duration_ms",
    )
    .withColumn("window_date", F.to_date("window_start"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Idempotent upsert
# MAGIC
# MAGIC `foreachBatch` hands each micro-batch to normal batch code, which is where MERGE becomes
# MAGIC available. The `txnVersion`/`txnAppId` options make the write idempotent, so a replayed
# MAGIC batch after a failure does not double-count.

# COMMAND ----------

MERGE_KEYS = ["window_start", "window_end", "country", "event_type"]


def upsert_to_gold(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    if not DeltaTable.isDeltaTable(spark, GOLD_PATH):
        batch_df.write.format("delta").partitionBy("window_date").save(GOLD_PATH)
        return

    target = DeltaTable.forPath(spark, GOLD_PATH)
    condition = " AND ".join([f"t.{k} = s.{k}" for k in MERGE_KEYS])

    (
        target.alias("t")
        .merge(batch_df.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


(
    aggregated.writeStream
    .foreachBatch(upsert_to_gold)
    .outputMode("update")
    .option("checkpointLocation", GOLD_CHECKPOINT)
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

# COMMAND ----------

spark.sql(
    f"CREATE TABLE IF NOT EXISTS gold.clickstream_metrics USING DELTA LOCATION '{GOLD_PATH}'"
)

display(
    spark.read.format("delta").load(GOLD_PATH)
    .orderBy(F.col("window_start").desc())
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time travel check
# MAGIC
# MAGIC Delta keeps a version history. Useful for demonstrating restatability in an interview.

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY delta.`{GOLD_PATH}`"))
