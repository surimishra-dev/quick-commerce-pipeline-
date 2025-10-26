import asyncio
import re
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lower, trim, when, initcap, lit, from_json
)
from pyspark.sql.types import (
    DecimalType, StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)

# === Create SparkSession ===
def get_spark_session():
    """Create and return a SparkSession configured for GCS access."""
    spark = (
        SparkSession.builder
        .appName("DataTransformationJobs")
        .master("local[*]")  # Use local mode for testing
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        # Uncomment the below line if running locally (not on Dataproc)
        # .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/path/to/service-account.json")
        .getOrCreate()
    )
    return spark


# === Helper: Find the most recent file in GCS folder ===
def get_latest_file(spark, folder_path):
    """
    Find the most recent CSV file in a GCS folder.
    Works both in Dataproc and locally if GCS connector is configured.
    """
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.org.apache.hadoop.fs.Path(folder_path).toUri(), hadoop_conf
        )

        status = fs.globStatus(spark._jvm.org.apache.hadoop.fs.Path(folder_path + "/*.csv"))
        files = [str(file.getPath().toString()) for file in status if file.isFile()]
        if not files:
            raise FileNotFoundError(f"No CSV files found in {folder_path}")

        if len(files) == 1:
            return files[0]

        def extract_ts(filename):
            match = re.search(r"(\d{8}_\d{6})", filename)
            if match:
                try:
                    return datetime.strptime(match.group(1), "%Y%m%d_%H%M%S")
                except:
                    return datetime.min
            return datetime.min

        latest_file = max(files, key=extract_ts)
        print(f"📄 Latest file detected: {latest_file}")
        return latest_file

    except Exception as e:
        raise Exception(f"Error finding latest file in {folder_path}: {e}")


# === Orders Transformation ===
async def transform_orders_data(spark):
    folder_path = "gs://dataproc-staging-asia-south1-297094044725-gxm4u7vu/orders_data/"
    input_path = get_latest_file(spark, folder_path)
    output_path = folder_path + "transformed_orders"

    print("✅ Starting Orders Transformation Job...")
    orders_df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
    print(f"📥 Orders Data Loaded: {orders_df.count()} records")

    orders_df = (
        orders_df.dropDuplicates(["order_id"])
        .filter(lower(trim(col("status"))) != "test")
        .withColumn("order_ts", col("order_ts").cast("timestamp"))
        .withColumn(
            "total_order_value",
            (col("quantity") * col("price").cast(DecimalType(10, 2))).alias("total_order_value")
        )
        .withColumn(
            "status",
            when(lower(trim(col("status"))).isin("delivered", "complete"), "delivered")
            .when(lower(trim(col("status"))).isin("cancelled", "canceled"), "cancelled")
            .when(lower(trim(col("status"))).isin("pending", "in progress", "processing"), "pending")
            .otherwise("unknown")
        )
    )

    (
        orders_df.write.mode("overwrite")
        .option("header", "true")
        .csv(output_path)
    )
    print(f"✅ Orders transformation complete! Output written to: {output_path}")


# === Inventory Transformation ===
async def transform_inventory_data(spark):
    folder_path = "gs://dataproc-staging-asia-south1-297094044725-gxm4u7vu/Inventorydata/"
    input_path = get_latest_file(spark, folder_path)
    output_path = folder_path + "transformed_inventory"

    print("✅ Starting Inventory Transformation Job...")

    schema = StructType([
        StructField("item_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("warehouse", StringType(), True),
        StructField("stock_level", IntegerType(), True),
        StructField("last_update", StringType(), True),
    ])

    inventory_df = spark.read.option("header", "true").schema(schema).csv(input_path)
    print(f"📥 Inventory Data Loaded: {inventory_df.count()} records")

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

    (
        inventory_df.write.mode("overwrite")
        .option("header", "true")
        .csv(output_path)
    )
    print(f"✅ Inventory transformation complete! Output written to: {output_path}")


# === Status Events Transformation ===
async def transform_status_events_data(spark):
    folder_path = "gs://dataproc-staging-asia-south1-297094044725-gxm4u7vu/StatusEventData/"
    input_path = get_latest_file(spark, folder_path)
    orders_path = "gs://dataproc-staging-asia-south1-297094044725-gxm4u7vu/orders_data/transformed_orders"
    output_path = folder_path + "transformed_status_events"

    print("✅ Starting Status Events Transformation Job...")

    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("courier_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("ts", StringType(), True),
    ])

    status_df = spark.read.option("header", "true").schema(schema).csv(input_path)
    print(f"📥 Status Events Loaded: {status_df.count()} records")

    orders_df = spark.read.option("header", "true").csv(orders_path).select("order_id")
    print(f"📋 Orders Data Loaded for Validation: {orders_df.count()} records")

    status_df = status_df.join(orders_df, on="order_id", how="inner")

    status_df = status_df.withColumn(
        "status",
        when(lower(trim(col("status"))).isin("picked_up", "pickup", "collected"), "picked_up")
        .when(lower(trim(col("status"))).isin("delivered", "completed"), "delivered")
        .when(lower(trim(col("status"))).isin("cancelled", "canceled"), "cancelled")
        .when(lower(trim(col("status"))).isin("in_transit", "on_the_way", "shipped"), "in_transit")
        .otherwise("unknown")
    )

    status_df = status_df.withColumn("ts", F.to_timestamp(F.col("ts"), "yyyy-MM-dd'T'HH:mm:ssX"))
    status_df = status_df.orderBy("order_id", "ts")

    (
        status_df.write.mode("overwrite")
        .option("header", "true")
        .csv(output_path)
    )
    print(f"✅ Status Events transformation complete! Output written to: {output_path}")


# === GPS Events Transformation ===
async def transform_gps_events_data(spark):
    folder_path = "gs://dataproc-staging-asia-south1-297094044725-gxm4u7vu/GPSEventData/"
    gps_input_path = get_latest_file(spark, folder_path)
    courier_details_path = "gs://dataproc-staging-asia-south1-297094044725-gxm4u7vu/StatusEventData/transformed_status_events/"
    output_path = folder_path + "transformed_gps_events"

    print("✅ Starting GPS Events Transformation Job...")

    schema = StructType([
        StructField("courier_id", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("ts", StringType(), True),
    ])

    gps_df = spark.read.option("header", "true").schema(schema).csv(gps_input_path)
    print(f"📥 GPS Events Loaded: {gps_df.count()} records")

    gps_df = gps_df.filter(
        (col("lat").isNotNull()) &
        (col("lon").isNotNull()) &
        (col("lat").between(-90, 90)) &
        (col("lon").between(-180, 180))
    )

    gps_df = gps_df.withColumn("event_time", F.to_timestamp(F.col("ts"), "yyyy-MM-dd'T'HH:mm:ssX")).drop("ts")

    courier_df = spark.read.option("header", "true").csv(courier_details_path)
    print(f"📦 Courier Details Loaded: {courier_df.count()} records")

    gps_df = gps_df.join(courier_df, on="courier_id", how="left")
    gps_df = gps_df.orderBy("courier_id", "event_time")

    (
        gps_df.write.mode("overwrite")
        .option("header", "true")
        .csv(output_path)
    )

    print(f"✅ GPS Events transformation complete! Output written to: {output_path}")


# === Main Entry Point ===
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
