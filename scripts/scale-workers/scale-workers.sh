#!/bin/bash

# Hardcoded thresholds
MIN_WORKERS=2
MAX_WORKERS=10

LOWER_THRESHOLD=10
UPPER_THRESHOLD=200

# RabbitMQ Config (loaded from ENV variables)
RABBITMQ_USER="${RABBITMQ_USER:-user}"
RABBITMQ_PASS="${RABBITMQ_PASS:-pass}"
RABBITMQ_HOST="${RABBITMQ_HOST:-rabbitmq}"
RABBITMQ_API_PORT="${RABBITMQ_API_PORT:-15672}"

exit 0

# Services and their queues
SERVICES=(
    "order order-egress"
    "payment payment-egress"
    "stock stock-egress"
)

# Function to get queue size
get_queue_size() {
    local queue_name=$1
    RESPONSE=$(curl -s -u "$RABBITMQ_USER:$RABBITMQ_PASS" \
      "http://$RABBITMQ_HOST:$RABBITMQ_API_PORT/api/queues/%2F/$queue_name")

    # Extract queue size using grep & sed
    QUEUE_SIZE=$(echo "$RESPONSE" | grep -o '"messages":[0-9]*' | sed 's/"messages"://')

    # Validate queue size
    if ! [[ "$QUEUE_SIZE" =~ ^[0-9]+$ ]]; then
        echo "Failed to fetch queue size for $queue_name, retrying..."
        echo 0
    else
        echo "$QUEUE_SIZE"
    fi
}

while true; do
    for SERVICE in "${SERVICES[@]}"; do
        read -r SERVICE_NAME QUEUE_NAME <<< "$SERVICE"

        # Get queue size
        QUEUE_SIZE=$(get_queue_size "$QUEUE_NAME")

        echo "Queue size for $SERVICE_NAME: $QUEUE_SIZE"

        # Get current worker count
        CURRENT_WORKERS=$(docker service ls --filter name="${SERVICE_NAME}-worker" --format '{{.Replicas}}' | cut -d'/' -f1)

        echo "Current Workers: $CURRENT_WORKERS"

        # Scale up
        if (( CURRENT_WORKERS < MIN_WORKERS || (QUEUE_SIZE > UPPER_THRESHOLD && CURRENT_WORKERS < MAX_WORKERS) )); then
            echo "Scaling Up!"
            NEW_WORKERS=$(( CURRENT_WORKERS + 1 ))
            docker service scale "${SERVICE_NAME}-worker=$NEW_WORKERS"

        # Scale down
        elif (( CURRENT_WORKERS > MAX_WORKERS || (QUEUE_SIZE < LOWER_THRESHOLD && CURRENT_WORKERS > MIN_WORKERS) )); then
            echo "Scaling Down!"
            NEW_WORKERS=$(( CURRENT_WORKERS - 1 ))
            docker service scale "${SERVICE_NAME}-worker=$NEW_WORKERS"

        else
            echo "[$SERVICE_NAME] Queue size $QUEUE_SIZE within range. Keeping $CURRENT_WORKERS workers."
        fi
    done

    sleep 10
done
