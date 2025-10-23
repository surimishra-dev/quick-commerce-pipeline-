import asyncio
from pyspark.sql.functions import to_timestamp
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
    input_path = "gs://dataproc-staging-asia-south1-297094044725-gxm4u7vu/Inventorydata/inventory_data_20251023_061656.csv"
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
    inventory_df = inventory_df.withColumn("last_update", try_to_timestamp("last_update", "yyyy-MM-dd'T'HH:mm:ssX"))


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


# === Main entrypoint ===
async def main():
    spark = get_spark_session()

    # Run both transformations asynchronously
    await asyncio.gather(
        transform_orders_data(spark),
        transform_inventory_data(spark)
    )

    spark.stop()


if __name__ == "__main__":
    asyncio.run(main())
