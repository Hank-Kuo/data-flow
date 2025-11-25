#!/bin/bash

# Setup and test Schema Registry with CDC

echo "🚀 Setting up Schema Registry with CDC..."

# Wait for Schema Registry to be ready
echo "⏳ Waiting for Schema Registry to be ready..."
until curl -f -s http://localhost:8085/subjects; do
  echo "Waiting for Schema Registry..."
  sleep 5
done

echo "✅ Schema Registry is ready!"

# Check current configuration
echo "📋 Schema Registry configuration:"
curl -s http://localhost:8085/config | jq '.'

# List current subjects (should be empty initially)
echo ""
echo "📝 Current subjects:"
curl -s http://localhost:8085/subjects | jq '.'

# Create Avro CDC connector (requires Confluent Platform connectors)
echo ""
echo "🔌 Note: To use Avro with Schema Registry, you need:"
echo "1. Confluent Platform Kafka Connect (not Debezium standalone)"
echo "2. Or custom Avro serializers"

echo ""
echo "📊 Current approach uses JSON CDC. To see schemas in action:"
echo "1. Update connector to use Avro (requires Confluent Connect)"
echo "2. Or manually register schemas"

# Example: Register a manual schema for demonstration
echo ""
echo "📝 Registering example customer schema..."

CUSTOMER_SCHEMA='{
  "schema": "{\"type\":\"record\",\"name\":\"Customer\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"first_name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"last_name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"email\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"created_at\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"updated_at\",\"type\":[\"null\",\"long\"],\"default\":null}]}"
}'

curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "$CUSTOMER_SCHEMA" \
  http://localhost:8085/subjects/inventory-customers-value/versions

echo ""
echo "✅ Example schema registered!"

# List subjects again
echo ""
echo "📝 Updated subjects:"
curl -s http://localhost:8085/subjects | jq '.'

# Get the schema we just registered
echo ""
echo "📖 Customer schema details:"
curl -s http://localhost:8085/subjects/inventory-customers-value/versions/latest | jq '.'

echo ""
echo "🎯 Schema Registry setup completed!"
echo ""
echo "📍 Schema Registry UI: http://localhost:8085"
echo "📍 Available endpoints:"
echo "  - GET  /subjects - list all subjects"
echo "  - GET  /subjects/{subject}/versions - list versions for subject"
echo "  - GET  /subjects/{subject}/versions/{version} - get specific schema"
echo "  - POST /subjects/{subject}/versions - register new schema"
echo ""
echo "💡 To use Avro CDC with Schema Registry:"
echo "1. Use Confluent Platform Kafka Connect"
echo "2. Or implement custom Avro serialization in Spark"
echo "3. Current setup uses JSON for simplicity"