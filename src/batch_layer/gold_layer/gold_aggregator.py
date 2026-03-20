"""
Batch Layer - Gold Aggregator
Production job for creating analytics-ready Gold tables.

Creates:
1. Item-Property Summary (most enriched items)
2. Property-Level Statistics (property distribution)

Ready for BI tools and dashboards.

Usage:
  python gold_aggregator.py --source dev.silver.batch_item_properties
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, min_by, max_by, 
    collect_list, datediff, current_timestamp, round
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_gold_tables(spark, source_table):
    """
    Create Gold analytics tables from Silver source.
    
    Args:
        spark: SparkSession
        source_table: Source Silver table
    """
    logger.info(f"Starting Gold aggregation from {source_table}")
    
    gold_table_1 = "dev.gold.item_properties_summary"
    gold_table_2 = "dev.gold.property_statistics"
    
    # Gold 1: Item-Property Summary
    logger.info(f"Creating {gold_table_1}")
    item_summary = spark.sql(f"""
        SELECT 
            item_id,
            COUNT(DISTINCT property_id) as property_count,
            COUNT(*) as total_values,
            COLLECT_LIST(DISTINCT property_id) as property_ids,
            MIN(event_time) as first_seen,
            MAX(event_time) as last_seen,
            DATEDIFF(MAX(event_time), MIN(event_time)) as days_active,
            CURRENT_TIMESTAMP() as computed_at
        FROM {source_table}
        GROUP BY item_id
    """)
    
    item_summary.coalesce(4).write.format("delta").mode("overwrite") \
        .option("mergeSchema", "true").saveAsTable(gold_table_1)
    
    item_count = item_summary.count()
    logger.info(f"✅ {gold_table_1}: {item_count} items")
    
    # Gold 2: Property-Level Statistics
    logger.info(f"Creating {gold_table_2}")
    prop_stats = spark.sql(f"""
        SELECT 
            property_id,
            COUNT(DISTINCT item_id) as items_with_property,
            COUNT(*) as total_property_records,
            COUNT(DISTINCT property_value) as distinct_values,
            MIN(event_time) as first_seen,
            MAX(event_time) as last_seen,
            ROUND(COUNT(*) / SUM(COUNT(*)) OVER (), 4) as frequency_ratio,
            CURRENT_TIMESTAMP() as computed_at
        FROM {source_table}
        GROUP BY property_id
    """)
    
    prop_stats.coalesce(4).write.format("delta").mode("overwrite") \
        .option("mergeSchema", "true").saveAsTable(gold_table_2)
    
    prop_count = prop_stats.count()
    logger.info(f"✅ {gold_table_2}: {prop_count} properties")
    
    return item_count, prop_count


def validate_gold(spark):
    """Validate Gold tables exist and have data."""
    tables = [
        "dev.gold.item_properties_summary",
        "dev.gold.property_statistics"
    ]
    
    for table in tables:
        try:
            count = spark.sql(f"SELECT COUNT(*) as count FROM {table}").collect()[0][0]
            logger.info(f"✅ {table}: {count} records")
        except Exception as e:
            logger.error(f"❌ {table}: {str(e)}")


def main():
    """Main entry point."""
    spark = SparkSession.builder.getOrCreate()
    
    source_table = "dev.silver.batch_silver"
    
    try:
        item_count, prop_count = create_gold_tables(spark, source_table)
        validate_gold(spark)
        logger.info(f"✅ Job completed. Created {item_count} items, {prop_count} properties")
        return 0
    except Exception as e:
        logger.error(f"❌ Job failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
