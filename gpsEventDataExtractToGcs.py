from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

PROJECT_ID = "flawless-agency-474210-p4"
SUBSCRIPTION = "GPSEvent-sub"
OUTPUT_PATH = "gs://dataproc-staging-asia-south1-925894589695-qxkvzrhv/GPSEventData/"

spark = (
    SparkSession.builder
        .appName("GPSEvent-PubSub-Streaming")
        .config("spark.jars.packages", "com.google.cloud.spark:spark-pubsub_2.12:0.4.2")
        .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = (
    StructType()
        .add("courier_id", StringType())
        .add("lat", DoubleType())
        .add("lon", DoubleType())
        .add("ts", StringType())
)

df_raw = (
    spark.readStream
        .format("pubsub")
        .option("projectId", PROJECT_ID)
        .option("subscription", SUBSCRIPTION)
        .load()
)

df_parsed = (
    df_raw
    .selectExpr("CAST(data AS STRING) as json_data")
    .select(from_json(col("json_data"), schema).alias("data"))
    .select("data.*")
)

df_final = df_parsed.withColumn("event_time", col("ts").cast(TimestampType()))

query = (
    df_final.writeStream
        .format("csv")
        .option("checkpointLocation", OUTPUT_PATH + "checkpoints/")
        .option("path", OUTPUT_PATH + "csv/")
        .option("header", "true")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
)

print("🚀 Spark streaming job started: reading from Pub/Sub and writing to GCS")
query.awaitTermination()
