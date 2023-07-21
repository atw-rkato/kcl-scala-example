#!/usr/bin/env bash
set -eu

# Set the table name and region
TABLE_NAME="sample-table"
export AWS_ACCESS_KEY_ID=dummyKey
export AWS_SECRET_ACCESS_KEY=dummySecret
export AWS_DEFAULT_REGION=ap-northeast-1

endpoint_url=http://localhost:49000

# Check if the table exists
if aws dynamodb describe-table --table-name "${TABLE_NAME}" --endpoint-url "${endpoint_url}" >/dev/null 2>&1; then
  echo "Table ${TABLE_NAME} already exists. Skipping creation."
else
  # Create the table with desired schema
  aws dynamodb create-table \
    --table-name "${TABLE_NAME}" \
    --attribute-definitions AttributeName=Id,AttributeType=S \
    --key-schema AttributeName=Id,KeyType=HASH \
    --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1 \
    --endpoint-url "${endpoint_url}" \
    --stream-specification StreamEnabled=true,StreamViewType=NEW_IMAGE \
  | cat
  echo "Table ${TABLE_NAME} created successfully."
fi
