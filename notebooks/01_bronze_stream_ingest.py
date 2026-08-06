# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze: Event Hubs → Delta
# MAGIC
# MAGIC Reads the `clickstream` Event Hub through its Kafka-compatible endpoint and writes the
# MAGIC **raw, unparsed** payload to Delta. Nothing is interpreted here on purpose: if the
# MAGIC downstream schema turns out to be wrong, Bronze still holds the original bytes and the
# MAGIC Silver layer can be rebuilt without re-ingesting.

# COMMAND ----------

from pyspark.sql import functions as F

# Secrets come from a Key Vault-backed scope — never hardcode a connection string.
EH_NAMESPACE = "your-namespace"
EH_TOPIC = "clickstream"
EH_CONN = dbutils.secrets.get(scope="kv-scope", key="eventhub-connection-string")

LAKE = "abfss://lakehouse@yourstorageaccount.dfs.core.windows.net"
BRONZE_PATH = f"{LAKE}/bronze/clickstream"
BRONZE_CHECKPOINT = f"{LAKE}/_checkpoints/bronze_clickstream"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Kafka connection
# MAGIC
# MAGIC Event Hubs exposes a Kafka surface on port 9093. The username is the literal string
# MAGIC `$ConnectionString` and the password is the connection string itself — an Event Hubs
# MAGIC convention, not a placeholder to substitute.

# COMMAND ----------

EH_SASL = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="$ConnectionString" password="{EH_CONN}";'
)

kafka_options = {
    "kafka.bootstrap.servers": f"{EH_NAMESPACE}.servicebus.windows.net:9093",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": EH_SASL,
    "kafka.request.timeout.ms": "60000",
    "kafka.session.timeout.ms": "30000",
    "subscribe": EH_TOPIC,
    "startingOffsets": "earliest",
    # Bounds each micro-batch so a backlog cannot produce one enormous batch.
    "maxOffsetsPerTrigger": "5000",
}

# COMMAND ----------

raw_stream = spark.readStream.format("kafka").options(**kafka_options).load()

bronze_df = raw_stream.select(
    F.col("key").cast("string").alias("event_key"),
    F.col("value").cast("string").alias("raw_payload"),
    F.col("topic").alias("source_topic"),
    F.col("partition").alias("source_partition"),
    F.col("offset").alias("source_offset"),
    F.col("timestamp").alias("enqueued_at"),
    F.current_timestamp().alias("ingested_at"),
    F.to_date(F.col("timestamp")).alias("ingest_date"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write
# MAGIC
# MAGIC `availableNow=True` drains whatever is currently in the hub and stops. Swap it for
# MAGIC `processingTime="30 seconds"` for a genuinely continuous stream — but remember the
# MAGIC cluster then runs until you stop it.

# COMMAND ----------

(
    bronze_df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", BRONZE_CHECKPOINT)
    .partitionBy("ingest_date")
    .trigger(availableNow=True)
    # .trigger(processingTime="30 seconds")
    .start(BRONZE_PATH)
    .awaitTermination()
)

# COMMAND ----------

spark.sql(
    f"CREATE TABLE IF NOT EXISTS bronze.clickstream USING DELTA LOCATION '{BRONZE_PATH}'"
)
display(spark.read.format("delta").load(BRONZE_PATH).limit(10))
