# streaming_gold.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, approx_count_distinct, collect_list, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Streaming_Gold").getOrCreate()

# Paths
silver_path = "dev.silver_stream.streaming_silver_events"

# Gold Visitor Profile
gold_profile_table = "dev.gold_stream.streaming_gold_visitor_profile"
gold_profile_checkpoint = "/Volumes/dev/gold_stream/checkpoint/streaming_gold_visitor_profile/"

silver_df = spark.readStream.format("delta").table(silver_path)

visitor_profile_df = silver_df.groupBy("visitorid").agg(
    count(when(col("event")=="view", True)).alias("total_viewcount"),
    approx_count_distinct(when(col("event")=="view", col("itemid"))).alias("unique_items_viewed"),
    count(when(col("event")=="addtocart", True)).alias("total_addtocart"),
    (count(when(col("event")=="transaction", True))>0).cast("int").alias("bought_something")
)

profile_query = (
    visitor_profile_df.writeStream.format("delta")
    .outputMode("complete")
    .trigger(availableNow=True)
    .option("checkpointLocation", gold_profile_checkpoint)
    .toTable(gold_profile_table)
)

# Gold Customer Journey
gold_journey_table = "dev.gold_stream.streaming_gold_customer_journey"
gold_journey_checkpoint = "/Volumes/dev/gold_stream/checkpoint/streaming_gold_customer_journey/"

window_spec = Window.partitionBy("visitorid","itemid").orderBy("event_time")
customer_journey_df = silver_df.withColumn("event_order", row_number().over(window_spec))

journey_query = (
    customer_journey_df.writeStream.format("delta")
    .outputMode("append")
    .trigger(availableNow=True)
    .option("checkpointLocation", gold_journey_checkpoint)
    .toTable(gold_journey_table)
)

# Gold Item Co-occurrence
gold_reco_table = "dev.gold_stream.streaming_gold_item_cooccurrence"
gold_reco_checkpoint = "/Volumes/dev/gold_stream/checkpoint/streaming_gold_item_cooccurrence/"

transaction_df = silver_df.filter(col("event")=="transaction")
item_cooccurrence_df = transaction_df.groupBy("transactionid").agg(
    collect_list("itemid").alias("items_bought_together")
)

reco_query = (
    item_cooccurrence_df.writeStream.format("delta")
    .outputMode("append")
    .trigger(availableNow=True)
    .option("checkpointLocation", gold_reco_checkpoint)
    .toTable(gold_reco_table)
)

# Keep streaming running
spark.streams.awaitAnyTermination()
