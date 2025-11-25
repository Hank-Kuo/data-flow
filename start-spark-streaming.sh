#!/bin/bash

# Start Spark Streaming for CDC Processing

echo "🚀 Starting Spark Streaming for CDC Processing..."

# Wait for Spark master to be ready
echo "⏳ Waiting for Spark services to be ready..."
sleep 30

# Check if Spark master is accessible
echo "🔍 Checking Spark master connectivity..."
if ! curl -f -s http://localhost:8088 > /dev/null; then
    echo "❌ Spark master is not accessible. Please make sure Spark services are running."
    exit 1
fi

echo "✅ Spark master is ready!"

# Submit the streaming application
echo "📡 Submitting Spark streaming application..."

echo "Available streaming applications:"
echo "1. simple_cdc_consumer.py - Basic CDC consumer"
echo "2. schema_aware_cdc_processor.py - Schema-aware CDC processor (recommended)"
echo "3. cdc_processor.py - Advanced CDC processor"

echo ""
echo "Starting schema-aware CDC processor..."

docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.driver.memory=1g \
    --conf spark.executor.memory=1g \
    /opt/bitnami/spark/streaming/schema_aware_cdc_processor.py

echo "✅ Spark streaming application started!"

echo ""
echo "🎯 To test the streaming application:"
echo "1. Open another terminal"
echo "2. Run: docker exec postgres-cdc psql -U postgres -d inventory -c \"UPDATE inventory.customers SET email = 'test@example.com' WHERE id = 1;\""
echo "3. Watch the Spark application logs for real-time processing"
echo ""
echo "🖥️  Monitor UIs:"
echo "   - Spark Master UI: http://localhost:8088"
echo "   - Jupyter Notebook: http://localhost:8888" 
echo "   - Kafka UI: http://localhost:8086"