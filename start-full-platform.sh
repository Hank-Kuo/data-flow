#!/bin/bash

# Complete startup script for the entire data flow platform

echo "🚀 Starting Complete Data Flow Platform..."

echo ""
echo "📋 This will start:"
echo "  ✅ Kafka + Zookeeper"
echo "  ✅ Schema Registry"
echo "  ✅ PostgreSQL CDC"
echo "  ✅ Debezium Kafka Connect"
echo "  ✅ Monitoring UIs"
echo "  ✅ Spark Cluster"
echo "  ✅ Jupyter Notebook"

echo ""
echo "⏳ Starting core Kafka infrastructure..."

# Start Kafka infrastructure
docker compose up -d zookeeper kafka schema-registry

echo "⏳ Waiting for Kafka infrastructure to be ready..."
sleep 45

# Start PostgreSQL and Kafka Connect
echo "⏳ Starting PostgreSQL CDC and Kafka Connect..."
docker compose up -d postgres-cdc kafka-connect

echo "⏳ Waiting for CDC services to be ready..."
sleep 30

# Start monitoring UIs
echo "⏳ Starting monitoring UIs..."
docker compose up -d kafka-ui debezium-ui

# Start Spark services
echo "⏳ Starting Spark cluster..."
docker compose up -d spark-master spark-worker jupyter-spark

echo "⏳ Waiting for all services to be ready..."
sleep 60

# Setup CDC and Schema Registry
echo "🔧 Setting up CDC and Schema Registry..."
./setup-cdc.sh

echo ""
echo "🎉 Platform startup completed!"
echo ""
echo "🌐 Access Points:"
echo "┌──────────────────────────────────────────────────────┐"
echo "│  Service              │  URL                         │"
echo "├──────────────────────────────────────────────────────┤"
echo "│  Kafka UI             │  http://localhost:8086       │"
echo "│  Debezium UI          │  http://localhost:8087       │"
echo "│  Schema Registry      │  http://localhost:8085       │"
echo "│  Spark Master         │  http://localhost:8088       │"
echo "│  Jupyter Notebook     │  http://localhost:8888       │"
echo "│  PostgreSQL CDC       │  localhost:5433 (postgres)  │"
echo "└──────────────────────────────────────────────────────┘"

echo ""
echo "🧪 Quick Test Commands:"
echo ""
echo "1️⃣  Test CDC:"
echo "   docker exec postgres-cdc psql -U postgres -d inventory -c \"UPDATE inventory.customers SET email = 'test@example.com' WHERE id = 1;\""

echo ""
echo "2️⃣  Monitor Kafka messages:"
echo "   docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic inventory-server.inventory.customers --from-beginning"

echo ""
echo "3️⃣  Check Schema Registry:"
echo "   curl -s http://localhost:8085/subjects | jq '.'"

echo ""
echo "4️⃣  Start Spark Streaming:"
echo "   ./start-spark-streaming.sh"

echo ""
echo "🔍 Service Status Check:"
docker compose ps

echo ""
echo "✅ Platform is ready for data processing!"