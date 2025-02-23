#!/bin/bash

# Define the Redis port
REDIS_PORT=6379

# Hardcoded container names for master and replica for each domain
order_masters=("order-master-1" "order-master-2" "order-master-3")
order_replicas=("order-replica-1" "order-replica-2" "order-replica-3")

payment_masters=("payment-master-1" "payment-master-2" "payment-master-3")
payment_replicas=("payment-replica-1" "payment-replica-2" "payment-replica-3")

stock_masters=("stock-master-1" "stock-master-2" "stock-master-3")
stock_replicas=("stock-replica-1" "stock-replica-2" "stock-replica-3")

echo "WAITING TO STOP RACE CONDITIONS"
sleep 1
echo "RUNNING CLUSTER SETUP"

# Function to extract IPs for Redis containers based on hardcoded names
get_hardcoded_ips() {
  local container_names=("$@")

  for container_name in "${container_names[@]}"; do
    container_ip=$(docker inspect --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$container_name")
    echo "$container_ip"
  done
}

# Create Redis cluster for a specific set of containers (order, payment, or stock)
create_redis_cluster() {
  local cluster_name=$1
  local masters=("${!2}")
  local replicas=("${!3}")

  # Get the IPs for the master and replica containers
  ips=$(get_hardcoded_ips "${masters[@]}" "${replicas[@]}")

  # Convert the IPs to an array
  ip_array=($ips)

  # Check if there are exactly 6 IPs (3 masters and 3 replicas)
  if [ ${#ip_array[@]} -ne 6 ]; then
    echo "Error: Expected exactly 6 Redis containers for $cluster_name cluster (3 masters and 3 replicas), but found ${#ip_array[@]}"
    exit 1
  fi

#  for ip in "${ip_array[@]}"; do
#    echo "Flushing all keys on Redis node $ip"
#    yes | redis-cli -h $ip -p $REDIS_PORT flushall
#  done

  echo "Creating Redis cluster for $cluster_name..."

  # Create the Redis cluster with the IPs
  yes "yes" | redis-cli --cluster create \
      ${ip_array[0]}:$REDIS_PORT ${ip_array[1]}:$REDIS_PORT ${ip_array[2]}:$REDIS_PORT \
      ${ip_array[3]}:$REDIS_PORT ${ip_array[4]}:$REDIS_PORT ${ip_array[5]}:$REDIS_PORT \
      --cluster-replicas 1
}

# Create clusters for order, payment, and stock
create_redis_cluster "order" order_masters[@] order_replicas[@]
create_redis_cluster "payment" payment_masters[@] payment_replicas[@]
create_redis_cluster "stock" stock_masters[@] stock_replicas[@]

exit 0
