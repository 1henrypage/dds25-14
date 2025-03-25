#!/bin/bash

# Hardcoded thresholds
MIN_WORKERS=10
MAX_WORKERS=20

LOWER_THRESHOLD=100
UPPER_THRESHOLD=500

# RabbitMQ Config (loaded from ENV variables)
RABBITMQ_USER="${RABBITMQ_USER:-user}"
RABBITMQ_PASS="${RABBITMQ_PASS:-pass}"
RABBITMQ_HOST="${RABBITMQ_HOST:-rabbitmq}"
RABBITMQ_API_PORT="${RABBITMQ_API_PORT:-15672}"

# Services and their queues
SERVICES=(
    "order order-egress env/order.env order-worker"
    "payment payment-egress env/payment.env payment-worker"
    "stock stock-egress env/stock.env stock-worker"
)

# Function to get queue size
get_queue_size() {
    local queue_name=$1
    RESPONSE=$(curl -s -u "$RABBITMQ_USER:$RABBITMQ_PASS" \
      "http://$RABBITMQ_HOST:$RABBITMQ_API_PORT/api/queues/%2F/$queue_name")

    QUEUE_SIZE=$(echo "$RESPONSE" | grep -o '"messages":[0-9]*' | sed 's/"messages"://')

    if ! [[ "$QUEUE_SIZE" =~ ^[0-9]+$ ]]; then
        echo "Failed to fetch queue size for $queue_name, retrying..."
        echo 0
    else
        echo "$QUEUE_SIZE"
    fi
}

get_current_workers() {
    local service_name=$1  # Use the service name for identifying the containers
    # Get the number of running containers whose name contains the service_name-worker
    CURRENT_WORKERS=$(docker ps --format "{{.Names}}" | grep -i "${service_name}-worker" | wc -l)
    echo "$CURRENT_WORKERS"
}


# Start scaling process
while true; do
    for SERVICE in "${SERVICES[@]}"; do
        read -r SERVICE_NAME QUEUE_NAME ENV_FILE IMAGE_NAME <<< "$SERVICE"

        QUEUE_SIZE=$(get_queue_size "$QUEUE_NAME")
        echo "Queue size for $SERVICE_NAME: $QUEUE_SIZE"

        # Get the current number of workers using the helper function
        CURRENT_WORKERS=$(get_current_workers "$SERVICE_NAME")
        echo "Current Workers: $CURRENT_WORKERS"

        # Scale logic
        if (( CURRENT_WORKERS < MIN_WORKERS || (QUEUE_SIZE > UPPER_THRESHOLD && CURRENT_WORKERS < MAX_WORKERS) )); then
            echo "Scaling Up!"
            # Use a random string for the worker name to ensure uniqueness
            RANDOM_SUFFIX=$(openssl rand -hex 4)
            CONTAINER_NAME="${SERVICE_NAME}-worker-${RANDOM_SUFFIX}"
            docker run -d --env-file "$ENV_FILE" -v ./common:/home/common --name "$CONTAINER_NAME" "$IMAGE_NAME"
        elif (( CURRENT_WORKERS > MAX_WORKERS || (QUEUE_SIZE < LOWER_THRESHOLD && CURRENT_WORKERS > MIN_WORKERS) )); then
            echo "Scaling Down!"
            # Find the oldest worker container by matching the name pattern
            OLDEST_WORKER=$(docker ps -q --filter "name=${SERVICE_NAME}-worker" --format "{{.ID}}" | head -n 1)
            if [[ -n "$OLDEST_WORKER" ]]; then
                docker stop "$OLDEST_WORKER"
            fi
        else
            echo "[$SERVICE_NAME] Queue size $QUEUE_SIZE within range. Keeping $CURRENT_WORKERS workers."
        fi
    done
    sleep 10
done
