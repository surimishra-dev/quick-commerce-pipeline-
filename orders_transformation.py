import asyncio
from pyspark.sql import functions as F
from pyspark.sql.functions import when, col, try_to_timestamp
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, trim, when, initcap, lit, from_json
)
from pyspark.sql.types import (
    DecimalType, StructType, StructField, StringType, IntegerType, TimestampType
)


def get_spark_session():
    """Create and return a SparkSession configured for GCS access."""
    spark = (
        SparkSession.builder
        .appName("DataTransformationJobs")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.jars", "gcs-connector.jar")  # Needed only when running outside Dataproc
        .master("local[*]")
        .getOrCreate()
    )
    return spark


# === Async function for orders transformation ===
# async def transform_orders_data(spark):
#     input_path = "gs://dataproc-staging-asia-south1-297094044725-gxm4u7vu/orders_data/orders_data_20251015_150651.csv"
#     output_path = "gs://dataproc-staging-asia-south1-297094044725-gxm4u7vu/orders_data/transformed_orders"

#     print("✅ Starting Orders Transformation Job...")

#     orders_df = (
#         spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
#     )

#     print(f"📥 Orders Data Loaded: {orders_df.count()} records")

#     # Clean + Normalize
#     orders_df = (
#         orders_df.dropDuplicates(["order_id"])
#         .filter(lower(trim(col("status"))) != "test")
#         .withColumn("order_ts", col("order_ts").cast("timestamp"))
#         .withColumn(
#             "total_order_value",
#             (col("quantity") * col("price").cast(DecimalType(10, 2))).alias("total_order_value")
#         )
#         .withColumn(
#             "status",
#             when(lower(trim(col("status"))).isin("delivered", "complete"), "delivered")
#             .when(lower(trim(col("status"))).isin("cancelled", "canceled"), "cancelled")
#             .when(lower(trim(col("status"))).isin("pending", "in progress", "processing"), "pending")
#             .otherwise("unknown")
#         )
#     )

#     # Write to GCS
#     (
#         orders_df.write.mode("overwrite")
#         .option("header", "true")
#         .csv(output_path)
#     )

#     print(f"✅ Orders transformation complete! Output written to: {output_path}")


# === Async function for inventory transformation ===
async def transform_inventory_data(spark):
    input_path = "gs://dataproc-staging-asia-south1-297094044725-gxm4u7vu/Inventorydata/inventory_data_20251023_094410.csv"
    output_path = "gs://dataproc-staging-asia-south1-297094044725-gxm4u7vu/Inventorydata/transformed_inventory"

    print("✅ Starting Inventory Transformation Job...")

    # Define schema (optional but recommended for CSV/JSON)
    schema = StructType([
        StructField("item_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("warehouse", StringType(), True),
        StructField("stock_level", IntegerType(), True),
        StructField("last_update", StringType(), True),
    ])

    inventory_df = (
        spark.read.option("header", "true").schema(schema).csv(input_path)
    )

    print(f"📥 Inventory Data Loaded: {inventory_df.count()} records")

    # === Standardize item names and categories ===
    inventory_df = (
        inventory_df
        .withColumn("name", initcap(trim(col("name"))))
        .withColumn("category", initcap(trim(col("category"))))
    )

    # === Handle missing stock levels ===
    inventory_df = (
        inventory_df
        .withColumn("stock_level", when(col("stock_level").isNull(), lit(0)).otherwise(col("stock_level")))
        .withColumn("stock_alert_flag", when(col("stock_level") == 0, lit(True)).otherwise(lit(False)))
    )

    # === Convert date fields to timestamp ===
    inventory_df = inventory_df.withColumn(
    "last_update",
    F.date_format(
        F.try_to_timestamp(F.trim(F.col("last_update").cast("string")), F.lit("dd-MM-yyyy HH:mm")),
        "yyyy-MM-dd'T'HH:mm:ss"
       )
     )



    # === Flatten nested JSON (if coming from API) ===
    # Example: if some field 'metadata' contains nested JSON
    if "metadata" in inventory_df.columns:
        json_schema = StructType([
            StructField("supplier", StringType(), True),
            StructField("expiry_date", StringType(), True)
        ])
        inventory_df = inventory_df.withColumn("metadata_parsed", from_json(col("metadata"), json_schema))
        for field in json_schema.fieldNames():
            inventory_df = inventory_df.withColumn(field, col(f"metadata_parsed.{field}"))
        inventory_df = inventory_df.drop("metadata", "metadata_parsed")

    # === Write output to GCS ===
    (
        inventory_df.write.mode("overwrite")
        .option("header", "true")
        .csv(output_path)
    )

    print(f"✅ Inventory transformation complete! Output written to: {output_path}")

# === Async function for Status Event transformation ===
async def transform_status_events_data(spark):
    input_path = "gs://dataproc-staging-asia-south1-297094044725-gxm4u7vu/StatusEventData/status_20251023_114332.csv"
    orders_path = "gs://dataproc-staging-asia-south1-297094044725-gxm4u7vu/orders_data/transformed_orders"
    output_path = "gs://dataproc-staging-asia-south1-297094044725-gxm4u7vu/StatusEventData/transformed_status_events"

    print("✅ Starting Status Events Transformation Job...")

    # === Define schema ===
    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("courier_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("ts", StringType(), True),
    ])

    # === Read status event data ===
    status_df = (
        spark.read.option("header", "true").schema(schema).csv(input_path)
    )
    print(f"📥 Status Events Loaded: {status_df.count()} records")

    # === Read transformed orders dataset for order_id validation ===
    orders_df = (
        spark.read.option("header", "true").csv(orders_path)
    ).select("order_id")

    print(f"📋 Orders Data Loaded for Validation: {orders_df.count()} records")

    # === Validate order IDs ===
    status_df = status_df.join(orders_df, on="order_id", how="inner")

    # === Normalize status values ===
    status_df = status_df.withColumn(
        "status",
        when(lower(trim(col("status"))).isin("picked_up", "pickup", "collected"), "picked_up")
        .when(lower(trim(col("status"))).isin("delivered", "completed"), "delivered")
        .when(lower(trim(col("status"))).isin("cancelled", "canceled"), "cancelled")
        .when(lower(trim(col("status"))).isin("in_transit", "on_the_way", "shipped"), "in_transit")
        .otherwise("unknown")
    )

    # === Convert timestamp to proper type ===
    status_df = status_df.withColumn(
        "ts",
        F.to_timestamp(F.col("ts"), "yyyy-MM-dd'T'HH:mm:ssX")
    )

    # === Sort events by timestamp to create timeline ===
    status_df = status_df.orderBy("order_id", "ts")

    # === Write the transformed dataset to GCS ===
    (
        status_df.write.mode("overwrite")
        .option("header", "true")
        .csv(output_path)
    )

    print(f"✅ Status Events transformation complete! Output written to: {output_path}")

async def transform_gps_events_data(spark):
    # === File paths ===
    gps_input_path = "gs://dataproc-staging-asia-south1-297094044725-gxm4u7vu/GPSEventData/gps_20251023_120049.csv"
    courier_details_path = "gs://dataproc-staging-asia-south1-297094044725-gxm4u7vu/StatusEventData/transformed_status_events/part-00000-72bdc5a6-c8d7-4b53-a584-e6b35a87a618-c000.csv"
    output_path = "gs://dataproc-staging-asia-south1-297094044725-gxm4u7vu/GPSEventData/transformed_gps_events"

    print("✅ Starting GPS Events Transformation Job...")

    # === Define schema ===
    schema = StructType([
        StructField("courier_id", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("ts", StringType(), True),
    ])

    # === Read GPS event data ===
    gps_df = (
        spark.read.option("header", "true")
        .schema(schema)
        .csv(gps_input_path)
    )
    print(f"📥 GPS Events Loaded: {gps_df.count()} records")

    # === Filter invalid or missing coordinates ===
    gps_df = gps_df.filter(
        (F.col("lat").isNotNull()) &
        (F.col("lon").isNotNull()) &
        (F.col("lat").between(-90, 90)) &
        (F.col("lon").between(-180, 180))
    )
    print(f"🧹 After Filtering Invalid Coordinates: {gps_df.count()} records")

    # === Convert timestamp to proper type ===
    gps_df = gps_df.withColumn(
        "event_time",
        F.to_timestamp(F.col("ts"), "yyyy-MM-dd'T'HH:mm:ssX")
    ).drop("ts")

    # === Read courier details CSV for join ===
    courier_df = (
        spark.read.option("header", "true")
        .csv(courier_details_path)
    )
    print(f"📦 Courier Details Loaded: {courier_df.count()} records")

    # === Join GPS data with courier details ===
    gps_df = gps_df.join(courier_df, on="courier_id", how="left")

    # === Sort events by courier_id and timestamp to create a timeline ===
    gps_df = gps_df.orderBy("courier_id", "event_time")

    # === Write transformed dataset to GCS (CSV format) ===
    (
        gps_df.write.mode("overwrite")
        .option("header", "true")
        .csv(output_path)
    )

    print(f"✅ GPS Events transformation complete! Output written to: {output_path}")





# === Main entrypoint ===
async def main():
    spark = get_spark_session()

    # Run both transformations asynchronously
    await asyncio.gather(
        # transform_orders_data(spark),
        # transform_inventory_data(spark),
        # transform_status_events_data(spark),
        transform_gps_events_data(spark)
    )

    spark.stop()


if __name__ == "__main__":
    asyncio.run(main())
