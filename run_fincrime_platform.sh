#!/bin/bash

# Configuration
KAFKA_DIR="./kafka_2.13-3.6.0"
REDIS_PORT=6379
KAFKA_PORT=9092
ZK_PORT=2181

# Function to kill process using a specific port or name
cleanup_component() {
    local NAME=$1
    local PORT=$2
    
    echo "Checking for existing $NAME..."
    # Kill by port if port is provided
    if [ ! -z "$PORT" ]; then
        fuser -k $PORT/tcp > /dev/null 2>&1
    fi
    # Kill by process name pattern to be safe
    pkill -f "$NAME" > /dev/null 2>&1
    sleep 1
}

# --- 1. RESET STATE ---
echo "--- Resetting FinCrime Detection Platform ---"

# Force remove the metadata and locks that cause Kafka to hang
rm -f /tmp/kafka-logs/meta.properties
rm -f /tmp/kafka-logs/.lock

# Kill existing Python scripts
pkill -f "streaming_pipeline.py"
pkill -f "kafka_to_redis_sink.py"

# 1. Kill everything
pkill -9 -f kafka.Kafka || true
pkill -9 -f QuorumPeerMain || true

# 2. Wipe ALL state
rm -rf /tmp/kafka-logs
rm -rf /tmp/zookeeper

# Cleanup Kafka and Zookeeper
cleanup_component "kafka.Kafka" $KAFKA_PORT
cleanup_component "QuorumPeerMain" $ZK_PORT

# Optional: Flush Redis if you want a clean feature store
redis-cli flushall > /dev/null 2>&1

echo "All components stopped. Starting fresh..."

# --- 2. START INFRASTRUCTURE ---

echo "[1/4] Starting Zookeeper..."
$KAFKA_DIR/bin/zookeeper-server-start.sh $KAFKA_DIR/config/zookeeper.properties > /tmp/zookeeper.log 2>&1 &

# Wait for Zookeeper to be fully functional (not just port-open)
echo "      -> Waiting for Zookeeper to be functional..."
while [ "$(echo ruok | nc localhost $ZK_PORT)" != "imok" ]; do
    echo "         (Zookeeper is booting...)"
    sleep 1
done
echo "      -> Zookeeper is UP and Ready."

# --- [2/4] Starting Kafka ---
echo "[2/4] Starting Kafka..."
# Kill any "ghost" Kafka process that might be hanging onto the port
pkill -9 -f kafka.Kafka || true

$KAFKA_DIR/bin/kafka-server-start.sh $KAFKA_DIR/config/server.properties > /tmp/kafka.log 2>&1 &

# Wait for Kafka with a timeout so you aren't stuck forever
MAX_WAIT=20
CUR_WAIT=0
while ! nc -z localhost $KAFKA_PORT; do
    sleep 1
    CUR_WAIT=$((CUR_WAIT+1))
    if [ $CUR_WAIT -ge $MAX_WAIT ]; then
        echo "ERROR: Kafka failed to start. Printing last 10 lines of /tmp/kafka.log:"
        tail -n 10 /tmp/kafka.log
        exit 1
    fi
done

# Give Kafka 2 seconds to elect a controller
sleep 2
echo "      -> Kafka is UP and Ready."


# Wait for Kafka
while ! nc -z localhost $KAFKA_PORT; do sleep 1; done

echo "--- Starting Fincrime Platform Setup ---"

BOOTSTRAP_SERVER="localhost:9092"
TOPICS=("raw-sentences" "word-counts")

echo "Checking Kafka topics..."
for TOPIC in "${TOPICS[@]}"; do
    $KAFKA_DIR/bin/kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --describe --topic $TOPIC > /dev/null 2>&1
    if [ $? -ne 0 ]; then
        echo "      -> Creating topic: $TOPIC"
        # Using 1 partition and replication factor 1 for local dev
        $KAFKA_DIR/bin/kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER \
            --create --topic $TOPIC \
            --partitions 1 --replication-factor 1
    else
        echo "      -> Topic '$TOPIC' already exists."
    fi
done


# --- 3. START PIPELINES ---
echo "[3/4] Starting Flink Pipeline..."
python3 ./streaming_platform/streaming_pipeline.py > /tmp/pipeline.log 2>&1 &

echo "[4/4] Starting Redis Sink..."
python3 kafka_to_redis_sink.py > /tmp/redis_sink.log 2>&1 &

# Final Check
sleep 3
echo "------------------------------------------------"
echo "Platform Reset Complete and Running."
echo "Use 'tail -f /tmp/pipeline.log' to monitor Flink."
echo "Use 'redis-cli monitor' to see real-time updates."
echo "------------------------------------------------"

# Launch Producer for interaction
$KAFKA_DIR/bin/kafka-console-producer.sh --topic raw-sentences --bootstrap-server localhost:9092
