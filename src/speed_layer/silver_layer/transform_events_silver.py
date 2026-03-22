# streaming_silver.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, when

spark = SparkSession.builder.appName("Streaming_Silver").getOrCreate()

# Paths
bronze_path = "dev.bronze_stream.bronze_streaming_item_properties"
silver_path = "dev.silver_stream.streaming_silver_events"
silver_checkpoint = "/Volumes/dev/silver_stream/checkpoint"

# Read Bronze stream
bronze_df = spark.readStream.format("delta").table(bronze_path)

# Silver Layer processing
silver_df = (
    bronze_df
    .withColumn("event_time", from_unixtime(col("timestamp") / 1000))
    .withColumn("transactionid", when(col("event") == "transaction", col("transactionid")).otherwise(None))
    .filter(col("event").isin(["view","addtocart","transaction"]))
)

# Write Silver stream
silver_query = (
    silver_df.writeStream.format("delta")
    .outputMode("append")
    .trigger(availableNow=True)
    .option("checkpointLocation", silver_checkpoint)
    .toTable(silver_path)
)

# Keep streaming running
spark.streams.awaitAnyTermination()
