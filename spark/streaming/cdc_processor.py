"""
Spark Streaming application to process CDC data from Kafka
"""

import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    """Create Spark session with Kafka support"""
    return SparkSession.builder \
        .appName("CDC-Processor") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def define_cdc_schema():
    """Define schema for CDC messages"""
    return StructType([
        StructField("before", StructType([
            StructField("id", IntegerType()),
            StructField("first_name", StringType()),
            StructField("last_name", StringType()),
            StructField("email", StringType()),
            StructField("created_at", LongType()),
            StructField("updated_at", LongType())
        ])),
        StructField("after", StructType([
            StructField("id", IntegerType()),
            StructField("first_name", StringType()),
            StructField("last_name", StringType()),
            StructField("email", StringType()),
            StructField("created_at", LongType()),
            StructField("updated_at", LongType())
        ])),
        StructField("source", StructType([
            StructField("version", StringType()),
            StructField("connector", StringType()),
            StructField("name", StringType()),
            StructField("ts_ms", LongType()),
            StructField("snapshot", StringType()),
            StructField("db", StringType()),
            StructField("schema", StringType()),
            StructField("table", StringType()),
            StructField("txId", LongType()),
            StructField("lsn", LongType())
        ])),
        StructField("op", StringType()),
        StructField("ts_ms", LongType()),
        StructField("transaction", StringType())
    ])

def process_customer_changes(spark):
    """Process customer table changes"""
    
    # Read from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "inventory-server.inventory.customers") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON and extract data
    cdc_schema = define_cdc_schema()
    
    parsed_df = df.select(
        col("key").cast("string"),
        from_json(col("value").cast("string"), cdc_schema).alias("data"),
        col("timestamp")
    )
    
    # Extract change information
    changes_df = parsed_df.select(
        col("timestamp").alias("kafka_timestamp"),
        col("data.op").alias("operation"),
        col("data.source.table").alias("table_name"),
        col("data.source.ts_ms").alias("source_timestamp"),
        col("data.before").alias("before_data"),
        col("data.after").alias("after_data")
    ).filter(col("operation").isNotNull())
    
    # Process different types of operations
    def process_batch(batch_df, batch_id):
        print(f"\n=== Processing Batch {batch_id} ===")
        
        if batch_df.count() > 0:
            # Count operations by type
            op_counts = batch_df.groupBy("operation").count().collect()
            print("Operation counts:")
            for row in op_counts:
                print(f"  {row['operation']}: {row['count']}")
            
            # Process inserts (snapshot or real inserts)
            inserts = batch_df.filter(col("operation").isin(["c", "r"]))
            if inserts.count() > 0:
                print(f"\nProcessing {inserts.count()} inserts/snapshots:")
                inserts.select("after_data.*").show(truncate=False)
            
            # Process updates
            updates = batch_df.filter(col("operation") == "u")
            if updates.count() > 0:
                print(f"\nProcessing {updates.count()} updates:")
                updates.select(
                    col("before_data.id").alias("customer_id"),
                    col("before_data.email").alias("old_email"),
                    col("after_data.email").alias("new_email"),
                    col("source_timestamp")
                ).show(truncate=False)
            
            # Process deletes
            deletes = batch_df.filter(col("operation") == "d")
            if deletes.count() > 0:
                print(f"\nProcessing {deletes.count()} deletes:")
                deletes.select("before_data.*").show(truncate=False)
        
        print("=== Batch processing completed ===\n")
    
    return changes_df.writeStream \
        .outputMode("append") \
        .foreachBatch(process_batch) \
        .trigger(processingTime="10 seconds") \
        .start()

def process_product_changes(spark):
    """Process product table changes"""
    
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "inventory-server.inventory.products") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Define product schema
    product_schema = StructType([
        StructField("before", StructType([
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("description", StringType()),
            StructField("price", DecimalType(10, 2)),
            StructField("quantity_in_stock", IntegerType()),
            StructField("category", StringType()),
            StructField("created_at", LongType()),
            StructField("updated_at", LongType())
        ])),
        StructField("after", StructType([
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("description", StringType()),
            StructField("price", DecimalType(10, 2)),
            StructField("quantity_in_stock", IntegerType()),
            StructField("category", StringType()),
            StructField("created_at", LongType()),
            StructField("updated_at", LongType())
        ])),
        StructField("source", StructType([
            StructField("table", StringType()),
            StructField("ts_ms", LongType())
        ])),
        StructField("op", StringType())
    ])
    
    parsed_df = df.select(
        from_json(col("value").cast("string"), product_schema).alias("data")
    )
    
    def process_product_batch(batch_df, batch_id):
        print(f"\n=== Processing Product Batch {batch_id} ===")
        
        if batch_df.count() > 0:
            # Price change alerts
            price_changes = batch_df.filter(col("data.op") == "u") \
                .select(
                    col("data.after.id").alias("product_id"),
                    col("data.after.name").alias("product_name"),
                    col("data.before.price").alias("old_price"),
                    col("data.after.price").alias("new_price")
                ).filter(col("old_price") != col("new_price"))
            
            if price_changes.count() > 0:
                print("🔔 Price changes detected:")
                price_changes.show(truncate=False)
            
            # Stock level alerts
            stock_changes = batch_df.filter(col("data.op") == "u") \
                .select(
                    col("data.after.id").alias("product_id"),
                    col("data.after.name").alias("product_name"),
                    col("data.before.quantity_in_stock").alias("old_stock"),
                    col("data.after.quantity_in_stock").alias("new_stock")
                ).filter(col("old_stock") != col("new_stock"))
            
            if stock_changes.count() > 0:
                print("📦 Stock changes detected:")
                stock_changes.withColumn(
                    "change", col("new_stock") - col("old_stock")
                ).show(truncate=False)
        
        print("=== Product batch processing completed ===\n")
    
    return parsed_df.writeStream \
        .outputMode("append") \
        .foreachBatch(process_product_batch) \
        .trigger(processingTime="10 seconds") \
        .start()

def create_aggregation_stream(spark):
    """Create real-time aggregations"""
    
    # Read all inventory changes
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "inventory-server.inventory.customers,inventory-server.inventory.products,inventory-server.inventory.orders") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Extract basic info
    parsed_df = df.select(
        col("topic"),
        col("timestamp"),
        get_json_object(col("value").cast("string"), "$.op").alias("operation"),
        get_json_object(col("value").cast("string"), "$.source.table").alias("table_name")
    ).filter(col("operation").isNotNull())
    
    # Real-time metrics
    metrics_df = parsed_df \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(
            window(col("timestamp"), "1 minute"),
            col("table_name"),
            col("operation")
        ).count()
    
    def output_metrics(batch_df, batch_id):
        if batch_df.count() > 0:
            print(f"\n📊 Real-time Metrics (Batch {batch_id}):")
            batch_df.orderBy("window", "table_name").show(truncate=False)
    
    return metrics_df.writeStream \
        .outputMode("update") \
        .foreachBatch(output_metrics) \
        .trigger(processingTime="30 seconds") \
        .start()

def main():
    """Main function to run all streaming applications"""
    
    print("🚀 Starting CDC Processor...")
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("✅ Spark session created")
    
    try:
        # Start multiple streaming queries
        print("📡 Starting streaming queries...")
        
        customer_query = process_customer_changes(spark)
        print("✅ Customer changes processor started")
        
        product_query = process_product_changes(spark)
        print("✅ Product changes processor started")
        
        metrics_query = create_aggregation_stream(spark)
        print("✅ Real-time metrics processor started")
        
        print("🔄 All streaming applications are running...")
        print("💡 You can now make changes to the database to see real-time processing!")
        print("💡 Example: docker exec postgres-cdc psql -U postgres -d inventory -c \"UPDATE inventory.customers SET email = 'new@email.com' WHERE id = 1;\"")
        
        # Wait for all queries to terminate
        customer_query.awaitTermination()
        product_query.awaitTermination()
        metrics_query.awaitTermination()
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()