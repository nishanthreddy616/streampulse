import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


input_uri = sys.argv[1]
output_table = sys.argv[2]
temporary_bucket = sys.argv[3]

spark = SparkSession.builder.appName("StreamPulseHistoricalBackfill").getOrCreate()

events = (
    spark.read.json(input_uri)
    .withColumn("occurred_at", F.to_timestamp("occurred_at"))
)

dedupe_window = Window.partitionBy("event_id").orderBy("occurred_at")

deduplicated = (
    events
    .withColumn("_row_number", F.row_number().over(dedupe_window))
    .filter(F.col("_row_number") == 1)
    .drop("_row_number")
)

session_features = (
    deduplicated
    .withColumn("metric_date", F.to_date("occurred_at"))
    .groupBy("metric_date", "title_id", "session_id")
    .agg(
        F.max(
            F.when(
                (F.col("event_type") == "play")
                & (F.col("duration_seconds") >= 30),
                1,
            ).otherwise(0)
        ).alias("qualified_start"),
        F.max(
            F.when(F.col("event_type") == "complete", 1).otherwise(0)
        ).alias("completed"),
        F.sum(
            F.when(
                F.col("event_type") == "buffer",
                F.col("duration_seconds"),
            ).otherwise(0)
        ).alias("buffering_seconds"),
        F.sum(
            F.when(
                F.col("event_type") == "play",
                F.col("duration_seconds"),
            ).otherwise(0)
        ).alias("viewing_seconds"),
    )
)

(
    session_features.write.format("bigquery")
    .option("temporaryGcsBucket", temporary_bucket)
    .option("table", output_table)
    .mode("overwrite")
    .save()
)

spark.stop()
