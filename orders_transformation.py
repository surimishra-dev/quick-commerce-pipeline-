import asyncio
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lower, trim, when, initcap, lit, from_json
)
from pyspark.sql.types import (
    DecimalType, StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)
import re


# ==========================================================
# Helper: Get Spark session
# ==========================================================
def get_spark_session():
    """Create and return a SparkSession configured for GCS access."""
    spark = (
        SparkSession.builder
        .appName("DataTransformationJobs")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.jars", "gcs-connector.jar")  # optional if local
        .master("local[*]")
        .getOrCreate()
    )
    return spark


# ==========================================================
# Helper: Find latest file in a GCS directory
# ==========================================================
def get_latest_file(spark, folder_path):
    """
    Lists all files in a GCS folder and returns the most recent one
    based on timestamp in filename or modified time.
    """
    try:
        # Get all files in the directory
        files_df = spark._jvm.org.apache.hadoop.fs.FileSystem \
            .get(spark._jsc.hadoopConfiguration()) \
            .listStatus(spark._jvm.org.apache.hadoop.fs.Path(folder_path))

        files = [str(file.getPath().toString()) for file in files_df if file.isFile()]
        if not files:
            raise FileNotFoundError(f"No files found in {folder_path}")

        # If only one file, return it
        if len(files) == 1:
            return files[0]

        # Try to pick the one with latest timestamp pattern in filename
        def extract_ts(path):
            match = re.search(r"(\d{8}_\d{6})", path)
            if match:
                try:
                    return datetime.strptime(match.group(1), "%Y%m%d_%H%M%S")
                except:
                    return datetime.min
            return datetime.min

        latest_file = max(files, key=extract_ts)
        return latest_file

    except Exception as e:
        raise Exception(f"Error finding latest file in {folder_path}: {e}")


# ==========================================================
# Orders Transformation
# ==========================================================
async def transform_orders_data(spark):
    folder_path = "gs://dataproc-staging-asia-south1-297094044725-gxm4u7vu/orders_data"
    input_path = get_latest_file(spark, folder_path)
    output_path = f"{folder_path}/transformed_orders"

    print(f"📁 Using Orders input: {input_path}")

    orders_df = (
        spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
    )

    print(f"📥 Loaded Orders Data: {orders_df.count()} records")

    orders_df = (
        orders_df.dropDuplicates(["order_id"])
        .filter(lower(trim(col("status"))) != "test")
        .withColumn("order_ts", col("order_ts").cast("timestamp"))
        .withColumn("total_order_value", (col("quantity") * col("price").cast(DecimalType(10, 2))))
        .withColumn(
            "status",
            when(lower(trim(col("status"))).isin("delivered", "complete"), "delivered")
            .when(lower(trim(col("status"))).isin("cancelled", "canceled"), "cancelled")
            .when(lower(trim(col("status"))).isin("pending", "in progress", "processing"), "pending")
            .otherwise("unknown")
        )
    )

    orders_df.write.mode("overwrite").option("header", "true").csv(output_path)
    print(f"✅ Orders transformation complete → {output_path}")


# ==========================================================
# Inventory Transformation
# ==========================================================
async def transform_inventory_data(spark):
    folder_path = "gs://dataproc-staging-asia-south1-297094044725-gxm4u7vu/Inventorydata"
    input_path = get_latest_file(spark, folder_path)
    output_path = f"{folder_path}/transformed_inventory"

    print(f"📁 Using Inventory input: {input_path}")

    schema = StructType([
        StructField("item_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("warehouse", StringType(), True),
        StructField("stock_level", IntegerType(), True),
        StructField("last_update", StringType(), True),
    ])

    inventory_df = spark.read.option("header", "true").schema(schema).csv(input_path)
    print(f"📥 Loaded Inventory Data: {inventory_df.count()} records")

    inventory_df = (
        inventory_df
        .withColumn("name", initcap(trim(col("name"))))
        .withColumn("category", initcap(trim(col("category"))))
        .withColumn("stock_level", when(col("stock_level").isNull(), lit(0)).otherwise(col("stock_level")))
        .withColumn("stock_alert_flag", when(col("stock_level") == 0, lit(True)).otherwise(lit(False)))
        .withColumn(
            "last_update",
            F.date_format(
                F.try_to_timestamp(F.trim(F.col("last_update").cast("string")), F.lit("dd-MM-yyyy HH:mm")),
                "yyyy-MM-dd'T'HH:mm:ss"
            )
        )
    )

    inventory_df.write.mode("overwrite").option("header", "true").csv(output_path)
    print(f"✅ Inventory transformation complete → {output_path}")


# ==========================================================
# Status Event Transformation
# ==========================================================
async def transform_status_events_data(spark):
    folder_path = "gs://dataproc-staging-asia-south1-297094044725-gxm4u7vu/StatusEventData"
    input_path = get_latest_file(spark, folder_path)
    orders_path = "gs://dataproc-staging-asia-south1-297094044725-gxm4u7vu/orders_data/transformed_orders"
    output_path = f"{folder_path}/transformed_status_events"

    print(f"📁 Using Status Events input: {input_path}")

    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("courier_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("ts", StringType(), True),
    ])

    status_df = spark.read.option("header", "true").schema(schema).csv(input_path)
    orders_df = spark.read.option("header", "true").csv(orders_path).select("order_id")

    status_df = (
        status_df.join(orders_df, on="order_id", how="inner")
        .withColumn(
            "status",
            when(lower(trim(col("status"))).isin("picked_up", "pickup", "collected"), "picked_up")
            .when(lower(trim(col("status"))).isin("delivered", "completed"), "delivered")
            .when(lower(trim(col("status"))).isin("cancelled", "canceled"), "cancelled")
            .when(lower(trim(col("status"))).isin("in_transit", "on_the_way", "shipped"), "in_transit")
            .otherwise("unknown")
        )
        .withColumn("ts", F.to_timestamp(F.col("ts"), "yyyy-MM-dd'T'HH:mm:ssX"))
        .orderBy("order_id", "ts")
    )

    status_df.write.mode("overwrite").option("header", "true").csv(output_path)
    print(f"✅ Status Events transformation complete → {output_path}")


# ==========================================================
# GPS Event Transformation
# ==========================================================
async def transform_gps_events_data(spark):
    folder_path = "gs://dataproc-staging-asia-south1-297094044725-gxm4u7vu/GPSEventData"
    input_path = get_latest_file(spark, folder_path)
    courier_details_path = "gs://dataproc-staging-asia-south1-297094044725-gxm4u7vu/StatusEventData/transformed_status_events"
    output_path = f"{folder_path}/transformed_gps_events"

    print(f"📁 Using GPS input: {input_path}")

    schema = StructType([
        StructField("courier_id", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("ts", StringType(), True),
    ])

    gps_df = spark.read.option("header", "true").schema(schema).csv(input_path)
    courier_df = spark.read.option("header", "true").csv(courier_details_path)

    gps_df = (
        gps_df.filter(
            (col("lat").isNotNull()) &
            (col("lon").isNotNull()) &
            (col("lat").between(-90, 90)) &
            (col("lon").between(-180, 180))
        )
        .withColumn("event_time", F.to_timestamp(F.col("ts"), "yyyy-MM-dd'T'HH:mm:ssX"))
        .drop("ts")
        .join(courier_df, on="courier_id", how="left")
        .orderBy("courier_id", "event_time")
    )

    gps_df.write.mode("overwrite").option("header", "true").csv(output_path)
    print(f"✅ GPS Events transformation complete → {output_path}")


# ==========================================================
# Main entrypoint
# ==========================================================
async def main():
    spark = get_spark_session()

    await asyncio.gather(
        transform_orders_data(spark),
        transform_inventory_data(spark),
        transform_status_events_data(spark),
        transform_gps_events_data(spark)
    )

    spark.stop()


if __name__ == "__main__":
    asyncio.run(main())
