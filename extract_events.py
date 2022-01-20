#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs"""

import json
from pyspark.sql import SparkSession


def main():
    """main
    """
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

    events = events_raw.select(events_raw.value.cast('string'))
    events_extracted = events.rdd.map(lambda x: json.loads(x.value)).toDF()

    events_extracted \
        .write \
        .parquet("/tmp/extracted_events")


if __name__ == "__main__":
    main()
