from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, when, round as spark_round
from pyspark.sql.types import DecimalType

def main():
    # === Spark Session Configuration ===
    spark = (
        SparkSession.builder
        .appName("OrdersTransformation")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.jars", "gcs-connector.jar")  # Needed only when running outside Dataproc
        .master("local[*]")  # Local Spark mode on your VM
        .getOrCreate()
    )

    # === Input & Output Paths (update folder names if needed) ===
    input_path = "gs://dataproc-staging-asia-south1-297094044725-gxm4u7vu/orders_data/orders_data_20251015_150651.csv"
    output_path = "gs://dataproc-staging-asia-south1-297094044725-gxm4u7vu/orders_data/transformed_orders"

    print("✅ Starting Orders Transformation Job...")

    # === Step 1: Read CSV File ===
    orders_df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
    )

    print("📥 Data Loaded from:", input_path)
    print(f"Total records before cleaning: {orders_df.count()}")

    # === Step 2: Remove duplicates & test entries ===
    orders_df = (
        orders_df
        .dropDuplicates(["order_id"])  # remove duplicate order IDs
        .filter(lower(trim(col("status"))) != "test")  # remove test entries
    )

    # === Step 3: Convert timestamps to proper datetime ===
    orders_df = orders_df.withColumn("order_ts", col("order_ts").cast("timestamp"))

    # === Step 4: Calculate total order value ===
    orders_df = orders_df.withColumn(
        "total_order_value",
        spark_round(col("quantity") * col("price").cast(DecimalType(10, 2)), 2)
    )

    # === Step 5: Normalize status values ===
    orders_df = orders_df.withColumn(
        "status",
        when(lower(trim(col("status"))).isin("delivered", "complete"), "delivered")
        .when(lower(trim(col("status"))).isin("cancelled", "canceled"), "cancelled")
        .when(lower(trim(col("status"))).isin("pending", "in progress", "processing"), "pending")
        .otherwise("unknown")
    )

    # === Step 6: Write output to GCS ===
    (
        orders_df.write.mode("overwrite")
        .option("header", "true")
        .csv(output_path)
    )

    print(f"✅ Transformation complete! Output written to: {output_path}")

    spark.stop()

if __name__ == "__main__":
    main()
