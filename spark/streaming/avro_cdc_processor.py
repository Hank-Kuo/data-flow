"""
Spark Streaming application to process Avro CDC data from Kafka with Schema Registry
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session_with_avro():
    """Create Spark session with Avro and Schema Registry support"""
    return SparkSession.builder \
        .appName("Avro-CDC-Processor") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
                "io.confluent:kafka-avro-serializer:7.4.0,"
                "io.confluent:kafka-schema-registry-client:7.4.0,"
                "org.apache.spark:spark-avro_2.12:3.4.0") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def process_avro_cdc_data(spark):
    """Process Avro CDC data from Kafka with Schema Registry"""
    
    # Read from Kafka with Avro
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "inventory-avro.inventory.customers") \
        .option("startingOffsets", "latest") \
        .option("kafka.security.protocol", "PLAINTEXT") \
        .load()
    
    # For Avro data with Schema Registry, we need to deserialize the value
    # This is a simplified approach - in production you'd use proper Avro deserialization
    avro_df = df.select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
        col("key").cast("string").alias("message_key"),
        # For now, we'll treat the value as binary and convert to string for inspection
        col("value").alias("avro_value")
    )
    
    def process_avro_batch(batch_df, batch_id):
        print(f"\n=== Processing Avro Batch {batch_id} ===")
        
        if batch_df.count() > 0:
            print(f"Received {batch_df.count()} Avro messages")
            
            # Show schema and sample data
            print("Schema:")
            batch_df.printSchema()
            
            print("Sample data:")
            batch_df.select("topic", "partition", "offset", "timestamp", "message_key").show(truncate=False)
            
            # In a real implementation, you would deserialize the Avro value here
            # using the schema from the Schema Registry
            print("Avro values (binary):")
            batch_df.select("avro_value").show(5, truncate=False)
        else:
            print("No new Avro messages")
        
        print("=== Avro batch processing completed ===\n")
    
    return avro_df.writeStream \
        .outputMode("append") \
        .foreachBatch(process_avro_batch) \
        .trigger(processingTime="10 seconds") \
        .start()

def main():
    """Main function to run Avro CDC processing"""
    
    print("🚀 Starting Avro CDC Processor with Schema Registry...")
    
    spark = create_spark_session_with_avro()
    spark.sparkContext.setLogLevel("WARN")
    
    print("✅ Spark session with Avro support created")
    
    try:
        # Start Avro streaming query
        print("📡 Starting Avro CDC streaming query...")
        
        avro_query = process_avro_cdc_data(spark)
        print("✅ Avro CDC processor started")
        
        print("🔄 Avro streaming application is running...")
        print("💡 To create Avro CDC connector:")
        print("curl -i -X POST -H 'Accept:application/json' -H 'Content-Type:application/json' http://localhost:8083/connectors/ -d @debezium-postgres-connector-avro.json")
        print("💡 Then try updating data:")
        print("docker exec postgres-cdc psql -U postgres -d inventory -c \"UPDATE inventory.customers SET email = 'avro-test@email.com' WHERE id = 1;\"")
        
        # Wait for query to terminate
        avro_query.awaitTermination()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()