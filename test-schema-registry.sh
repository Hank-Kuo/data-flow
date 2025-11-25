#!/bin/bash

# Test Schema Registry functionality

echo "🧪 Testing Schema Registry functionality..."

echo ""
echo "1️⃣ Checking Schema Registry health..."
curl -s http://localhost:8085/subjects | jq '.'

echo ""
echo "2️⃣ Listing all registered schemas..."
for subject in $(curl -s http://localhost:8085/subjects | jq -r '.[]'); do
    echo "📋 Subject: $subject"
    curl -s "http://localhost:8085/subjects/$subject/versions/latest" | jq '{version: .version, id: .id, schema: .schema | fromjson}'
    echo ""
done

echo ""
echo "3️⃣ Testing compatibility check..."
UPDATED_SCHEMA='{
  "schema": "{\"type\":\"record\",\"name\":\"Customer\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"first_name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"last_name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"email\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"phone\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"created_at\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"updated_at\",\"type\":[\"null\",\"long\"],\"default\":null}]}"
}'

echo "Testing backward compatibility (adding optional phone field)..."
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "$UPDATED_SCHEMA" \
  "http://localhost:8085/compatibility/subjects/inventory-customers-value/versions/latest" | jq '.'

echo ""
echo "4️⃣ Registering the compatible schema..."
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "$UPDATED_SCHEMA" \
  "http://localhost:8085/subjects/inventory-customers-value/versions" | jq '.'

echo ""
echo "5️⃣ Checking updated subject versions..."
curl -s "http://localhost:8085/subjects/inventory-customers-value/versions" | jq '.'

echo ""
echo "✅ Schema Registry test completed!"
echo "🔗 You can also access Schema Registry REST API at: http://localhost:8085"