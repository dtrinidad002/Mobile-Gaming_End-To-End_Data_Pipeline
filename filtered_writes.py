#!/usr/bin/env python
"""Python spark script for saving filtered events"""

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf
import json

@udf('boolean')
def is_purchase(event_as_json):
    event = json.loads(event_as_json)
    if event['event_type'] == 'purchase_sword':
        return True
    return False


def main():
    """main"""
    
    spark = SparkSession \
        .builder \
        .appName("ExtractEventsJob") \
        .getOrCreate()

    events_raw = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "events") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    event_purchases = events_raw \
        .select(events_raw.value.cast('string').alias('raw'),
                events_raw.timestamp.cast('string')) \
        .filter(is_purchase('raw'))

    event_join_guild = events_raw \
        .select(raw_events.value.cast('string').alias('raw'),
                raw_events.timestamp.cast('string')) \
        .filter(is_joinguild('raw'))
    
    event_purchases_extracted = event_purchases \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
        .toDF()
    event_purchases_extracted.printSchema()
    event_purchases_extracted.show()

    events_join_guild_extracted = event_joinGuild \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
        .toDF()
    event_join_guild_extracted.printSchema()
    event_join_guild_extracted.show()
    
    event_purchases_extracted \
        .write \
        .mode('overwrite') \
        .parquet('/tmp/purchases')


if __name__ == "__main__":
    main()
