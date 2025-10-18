from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lower, trim, to_timestamp, round as spark_round

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("OrdersDataTransformation") \
        .getOrCreate()

    # ---- Configuration ----
    input_path = "gs://dataproc-staging-asia-south1-297094044725-gxm4u7vu/orders_data/"
    output_path = "gs://dataproc-staging-asia-south1-297094044725-gxm4u7vu/orders_data_transformed/"
    
    # ---- Read CSV ----
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)
    
    # ---- Data Cleaning ----
    # 1️⃣ Remove duplicates and test entries
    df_cleaned = df.dropDuplicates(["order_id"]).filter(lower(trim(col("status"))) != "test")
    
    # 2️⃣ Convert timestamp to proper datetime
    df_cleaned = df_cleaned.withColumn("order_ts", to_timestamp(col("order_ts")))
    
    # 3️⃣ Calculate total order value (quantity * price)
    df_cleaned = df_cleaned.withColumn("total_order_value", spark_round(col("quantity") * col("price"), 2))
    
    # 4️⃣ Normalize status values
    df_cleaned = df_cleaned.withColumn(
        "status",
        when(lower(trim(col("status"))).isin("delivered", "completed"), "delivered")
        .when(lower(trim(col("status"))).isin("cancelled", "canceled"), "cancelled")
        .when(lower(trim(col("status"))).isin("pending", "in progress", "processing"), "pending")
        .otherwise("unknown")
    )
    
    # ---- Write Output ----
    df_cleaned.write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(output_path)
    
    print("✅ Transformation complete! Transformed data saved to:", output_path)

    spark.stop()

if __name__ == "__main__":
    main()
