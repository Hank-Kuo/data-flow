"""
Simple Spark Streaming application to consume CDC data from Kafka
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Simple-CDC-Consumer") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("🚀 Starting Simple CDC Consumer...")
    
    try:
        # Read from Kafka
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "inventory-server.inventory.customers") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Simple processing - just show the raw messages
        def process_batch(batch_df, batch_id):
            print(f"\n=== Batch {batch_id} ===")
            if batch_df.count() > 0:
                print(f"Received {batch_df.count()} messages:")
                
                # Parse and show operation type
                messages = batch_df.select(
                    col("timestamp"),
                    get_json_object(col("value").cast("string"), "$.op").alias("operation"),
                    get_json_object(col("value").cast("string"), "$.after.id").alias("customer_id"),
                    get_json_object(col("value").cast("string"), "$.after.email").alias("email")
                )
                
                messages.show(truncate=False)
            else:
                print("No new messages")
            print("=" * 30)
        
        # Start streaming
        query = df.writeStream \
            .outputMode("append") \
            .foreachBatch(process_batch) \
            .trigger(processingTime="5 seconds") \
            .start()
        
        print("✅ Streaming started! Waiting for CDC events...")
        print("💡 Try updating customer data:")
        print("docker exec postgres-cdc psql -U postgres -d inventory -c \"UPDATE inventory.customers SET email = 'test@email.com' WHERE id = 1;\"")
        
        query.awaitTermination()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()