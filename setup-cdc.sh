#!/bin/bash

# Setup script for Kafka CDC with PostgreSQL and Schema Registry

echo "🚀 Setting up Kafka CDC with PostgreSQL and Schema Registry..."

# Wait for services to be ready
echo "⏳ Waiting for services to start..."
sleep 30

# Check if Kafka is ready
echo "🔍 Checking Kafka status..."
until curl -f -s http://localhost:8086 > /dev/null; do
  echo "Waiting for Kafka to be ready..."
  sleep 5
done
echo "✅ Kafka is ready!"

# Check if Schema Registry is ready
echo "🔍 Checking Schema Registry status..."
until curl -f -s http://localhost:8085/subjects; do
  echo "Waiting for Schema Registry to be ready..."
  sleep 5
done
echo "✅ Schema Registry is ready!"

# Check if Kafka Connect is ready
echo "🔍 Checking Kafka Connect status..."
until curl -f -s http://localhost:8083/connectors; do
  echo "Waiting for Kafka Connect to be ready..."
  sleep 5
done
echo "✅ Kafka Connect is ready!"

# Setup Schema Registry with example schemas
echo "📝 Setting up Schema Registry..."
echo "Registering example customer schema..."

CUSTOMER_SCHEMA='{
  "schema": "{\"type\":\"record\",\"name\":\"Customer\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"first_name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"last_name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"email\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"created_at\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"updated_at\",\"type\":[\"null\",\"long\"],\"default\":null}]}"
}'

curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "$CUSTOMER_SCHEMA" \
  http://localhost:8085/subjects/inventory-customers-value/versions > /dev/null 2>&1

echo "✅ Schema Registry setup completed!"

# Create the Debezium PostgreSQL connector (JSON format)
echo "🔌 Creating Debezium PostgreSQL connector (JSON)..."
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  http://localhost:8083/connectors/ \
  -d @debezium-postgres-connector.json

echo ""
echo "📄 Connector creation request sent!"

# Wait a bit for connector to initialize
sleep 10

# Check connector status
echo "🔍 Checking connector status..."
curl -s http://localhost:8083/connectors/inventory-connector/status | jq '.'

echo ""
echo "📋 Listing all connectors..."
curl -s http://localhost:8083/connectors | jq '.'

echo ""
echo "📊 Schema Registry subjects:"
curl -s http://localhost:8085/subjects | jq '.'

echo ""
echo "🎉 CDC setup completed!"
echo ""
echo "🖥️  Available UIs:"
echo "  - Kafka UI: http://localhost:8086"
echo "  - Debezium UI: http://localhost:8087"
echo "  - Schema Registry: http://localhost:8085"
echo ""
echo "🗄️  Database connection:"
echo "  - Host: localhost"
echo "  - Port: 5433"
echo "  - Database: inventory"
echo "  - User: postgres"
echo "  - Password: postgres"
echo ""
echo "📚 Available connectors:"
echo "  - JSON CDC with Schema: debezium-postgres-connector.json (active - schemas enabled)"
echo "  - Avro CDC: debezium-postgres-connector-avro.json (for Confluent Platform)"
echo ""
echo "🧪 To test CDC, try updating some data:"
echo "  docker exec postgres-cdc psql -U postgres -d inventory -c \"UPDATE inventory.customers SET email = 'test@example.com' WHERE id = 1;\""
echo ""
echo "📈 Monitor changes:"
echo "  docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic inventory-server.inventory.customers --from-beginning"