#!/usr/bin/env python
"""Python Spark Script to separate the event messages"""

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf
import json

@udf('string')
def munge_event(event_as_json):
    event = json.loads(event_as_json)
    event['Host'] = "moe"
    event['Cache-Control'] = "no-cache"
    return json.dumps(event)


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

    events_munged = events_raw \
        .select(events_raw.value.cast('string').alias('raw'),
                events_raw.timestamp.cast('string')) \
        .withColumn('munged', events_munged('raw'))

    def select_keys(old_dict):
        keys = ["Host", "event_type", "Accept", "User-Agent", "color", "type"]
        return({key: old_dict.get(key, "") for key in keys })

    events_extracted = munged \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **select_keys(json.loads(r.munged)))) \
        .toDF()

    join_guild = events_extracted \
        .filter(events_extracted.event_type == 'join_guild')
    join_guild.show()
    join_guild \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/join_guild")
    
    purchase_sword = extracted_events \
        .filter(extracted_events.event_type == 'purchase_sword')
    purchase_sword.show()
    purchase_sword \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/purchase_sword")

if __name__ == "__main__":
    main()
