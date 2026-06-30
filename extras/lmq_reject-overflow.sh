#!/bin/bash

# Usage: ./script.sh "amqps://username:passwd@servername/vhostname"
# Or: AMQP_URL="amqps://username:passwd@servername/vhostname" ./script.sh

# Check if AMQP_URL is provided as argument or environment variable
if [ -n "$1" ]; then
  AMQP_URL="$1"
elif [ -z "$AMQP_URL" ]; then
  echo "Error: AMQP_URL not provided"
  echo "Usage: $0 \"amqps://username:passwd@servername/vhostname\""
  echo "Or: export AMQP_URL=\"amqps://username:passwd@servername/vhostname\" && $0"
  exit 1
fi

# Extract components from AMQP_URL
# Remove amqp:// or amqps:// prefix
TEMP_URL="${AMQP_URL#amqp://}"
TEMP_URL="${TEMP_URL#amqps://}"

# Extract credentials and host
if [[ $TEMP_URL == *@* ]]; then
  CREDENTIALS="${TEMP_URL%%@*}"
  HOST_AND_VHOST="${TEMP_URL##*@}"
else
  echo "Error: No credentials found in AMQP_URL"
  exit 1
fi

# Extract host and vhost (vhost comes after the first /)
if [[ $HOST_AND_VHOST == */* ]]; then
  HOST="${HOST_AND_VHOST%%/*}"
  VHOST="${HOST_AND_VHOST#*/}"
  # URL encode the vhost for API calls
  VHOST_ENCODED=$(printf %s "$VHOST" | jq -sRr @uri)
else
  HOST="$HOST_AND_VHOST"
  VHOST="/"
  VHOST_ENCODED="%2F"
fi

# Build HTTPS_URL for RabbitMQ Management API (will use port 443)
HTTPS_URL="http://${CREDENTIALS}@${HOST}:15672"

echo "Using HTTPS_URL: ${HTTPS_URL}"
echo "Virtual host: $VHOST (encoded as: $VHOST_ENCODED)"

# 1. Create Queue q1
echo -e "\n=== Creating queue q1 ==="
curl -i -u "$CREDENTIALS" -H "content-type:application/json" \
  -X PUT "${HTTPS_URL}/api/queues/${VHOST_ENCODED}/q1" \
  -d '{}'

# 2. Create Queue q2 with max-length and overflow policy
echo -e "\n=== Creating queue q2 with max-length=100 and x-overflow=reject-publish ==="
curl -i -u "$CREDENTIALS" -H "content-type:application/json" \
  -X PUT "${HTTPS_URL}/api/queues/${VHOST_ENCODED}/q2" \
  -d '{
    "arguments": {
      "x-max-length": 100,
      "x-overflow": "reject-publish"
    }
  }'

# Wait for user to trigger shovel creation
echo -e "\n=== Queues created successfully ==="
echo -e "\nPress ENTER to create the shovel from q1 to q2..."
read -r

# 3. Create a Shovel from q1 to q2
echo -e "\n=== Creating shovel from q1 to q2 ==="
curl -i -u "$CREDENTIALS" -H "content-type:application/json" \
  -X PUT "${HTTPS_URL}/api/parameters/shovel/${VHOST_ENCODED}/q1-to-q2-shovel" \
  -d '{
    "value": {
      "src-protocol": "amqp091",
      "src-uri": "'"$AMQP_URL"'",
      "src-queue": "q1",
      "dest-protocol": "amqp091",
      "dest-uri": "'"$AMQP_URL"'",
      "dest-queue": "q2"
    }
  }'

# Verify the setup
echo -e "\n=== Verifying queues ==="
curl -s -u "$CREDENTIALS" "${HTTPS_URL}/api/queues/${VHOST_ENCODED}" | grep -E '"name":|"x-max-length":|"x-overflow":'

echo -e "\n=== Verifying shovels ==="
curl -s -u "$CREDENTIALS" "${HTTPS_URL}/api/parameters/shovel/${VHOST_ENCODED}"

echo -e "\n=== Setup complete ==="
