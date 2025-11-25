"""
Spark Streaming application to process schema-aware CDC data from Kafka
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    """Create Spark session"""
    return SparkSession.builder \
        .appName("Schema-Aware-CDC-Processor") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

def process_schema_aware_cdc(spark):
    """Process CDC data with embedded JSON schema"""
    
    # Read from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "inventory-server.inventory.customers") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse the JSON value
    parsed_df = df.select(
        col("timestamp").alias("kafka_timestamp"),
        from_json(col("value").cast("string"), StructType([
            StructField("schema", StructType([
                StructField("type", StringType()),
                StructField("fields", ArrayType(StringType())),
                StructField("optional", BooleanType()),
                StructField("name", StringType()),
                StructField("version", IntegerType())
            ])),
            StructField("payload", StructType([
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
                    StructField("db", StringType()),
                    StructField("schema", StringType()),
                    StructField("table", StringType())
                ])),
                StructField("op", StringType()),
                StructField("ts_ms", LongType())
            ]))
        ])).alias("cdc_data")
    )
    
    # Extract the actual data
    processed_df = parsed_df.select(
        col("kafka_timestamp"),
        col("cdc_data.schema.name").alias("schema_name"),
        col("cdc_data.schema.version").alias("schema_version"),
        col("cdc_data.payload.op").alias("operation"),
        col("cdc_data.payload.source.table").alias("table_name"),
        col("cdc_data.payload.source.ts_ms").alias("source_timestamp"),
        col("cdc_data.payload.before").alias("before_data"),
        col("cdc_data.payload.after").alias("after_data")
    ).filter(col("operation").isNotNull())
    
    def process_schema_batch(batch_df, batch_id):
        print(f"\n=== Processing Schema-Aware Batch {batch_id} ===")
        
        if batch_df.count() > 0:
            print(f"📊 Received {batch_df.count()} schema-aware CDC messages")
            
            # Show schema information
            print("📋 Schema Information:")
            batch_df.select("schema_name", "schema_version").distinct().show(truncate=False)
            
            # Show operation statistics
            print("📈 Operation Statistics:")
            batch_df.groupBy("operation").count().show()
            
            # Process different operations
            updates = batch_df.filter(col("operation") == "u")
            if updates.count() > 0:
                print("🔄 Updates detected:")
                updates.select(
                    col("before_data.id").alias("customer_id"),
                    col("before_data.email").alias("old_email"),
                    col("after_data.email").alias("new_email"),
                    from_unixtime(col("source_timestamp")/1000).alias("change_time")
                ).show(truncate=False)
                
                # Detect email changes
                email_changes = updates.filter(
                    col("before_data.email") != col("after_data.email")
                )
                if email_changes.count() > 0:
                    print("📧 Email changes detected:")
                    email_changes.select(
                        col("after_data.id").alias("customer_id"),
                        col("after_data.first_name").alias("first_name"),
                        col("after_data.last_name").alias("last_name"),
                        col("before_data.email").alias("old_email"),
                        col("after_data.email").alias("new_email")
                    ).show(truncate=False)
            
            # Process inserts
            inserts = batch_df.filter(col("operation").isin(["c", "r"]))
            if inserts.count() > 0:
                print("➕ New customers:")
                inserts.select("after_data.*").show(truncate=False)
            
            # Process deletes
            deletes = batch_df.filter(col("operation") == "d")
            if deletes.count() > 0:
                print("🗑️ Deleted customers:")
                deletes.select("before_data.*").show(truncate=False)
                
        else:
            print("📭 No new schema-aware messages")
        
        print("=== Schema-aware batch processing completed ===\n")
    
    return processed_df.writeStream \
        .outputMode("append") \
        .foreachBatch(process_schema_batch) \
        .trigger(processingTime="10 seconds") \
        .start()

def main():
    """Main function"""
    
    print("🚀 Starting Schema-Aware CDC Processor...")
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("✅ Spark session created")
    
    try:
        print("📡 Starting schema-aware CDC streaming...")
        
        query = process_schema_aware_cdc(spark)
        print("✅ Schema-aware CDC processor started")
        
        print("🔄 Processing schema-aware CDC messages...")
        print("💡 Test with:")
        print("docker exec postgres-cdc psql -U postgres -d inventory -c \"UPDATE inventory.customers SET email = 'schema-test@example.com' WHERE id = 4;\"")
        
        query.awaitTermination()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()