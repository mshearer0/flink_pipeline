import pytest
import time
import json
import uuid
import redis
from kafka import KafkaProducer, KafkaConsumer
import os

# --- CONFIGURATION ---
SOURCE_TOPIC = "raw-sentences" 
SINK_TOPIC = "word-counts"
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = 6379
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

@pytest.fixture(scope="module")
def kafka_producer():
    """Sends a raw word as bytes to the source topic."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: str(v).encode('utf-8')
    )

@pytest.fixture(scope="module")
def kafka_consumer():
    import uuid
    # Generate a completely random group ID every single time the test runs
    random_group = f"debug_group_{uuid.uuid4().hex[:8]}"
    
    return KafkaConsumer(
        SINK_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        # 'earliest' + a brand new group_id forces Kafka to send all historical data
        auto_offset_reset='earliest',
        group_id=random_group,
        enable_auto_commit=False,  # Don't save progress, so we can re-run tests
        value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
        # Upsert-Kafka often uses keys; adding this prevents metadata errors
        key_deserializer=lambda x: x.decode('utf-8') if x else None,
        # Give the consumer more time to find the coordinator
        metadata_max_age_ms=5000 
    )

@pytest.fixture(scope="module")
def redis_client():
    """Connection to the Redis instance."""
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def test_word_count_pipeline(kafka_producer, kafka_consumer, redis_client):

    # Create a unique word
    unique_id = str(uuid.uuid4())[:8]
    probe_word = f"test_{unique_id}"

    # Write the word to the source topic
    print(f"\n[Step 1] Writing raw word: '{probe_word}' to {SOURCE_TOPIC}")
    kafka_producer.send(SOURCE_TOPIC, probe_word)
    kafka_producer.flush()

    # Check the Sink Topic (Kafka)
    print(f"[Step 2] Waiting for '{probe_word}' in {SINK_TOPIC}...")
    found_in_kafka = False
    start_time = time.time()
    
    # Wait up to 120 seconds for Flink to process
    while time.time() - start_time < 120:
        records = kafka_consumer.poll(timeout_ms=1000)
        for _, messages in records.items():
            for msg in messages:
                data = msg.value
                print(f"      -> Received message: {data}")
                
                # Check if this message is our probe
                if isinstance(data, dict) and data.get('word') == probe_word:
                    found_in_kafka = True
                    break
        if found_in_kafka:
            break

    assert found_in_kafka, f"TIMEOUT: '{probe_word}' did not arrive in Kafka topic '{SINK_TOPIC}'"
    print(f"✅ Found '{probe_word}' in Kafka!")


    # 3. Redis Verification
    print(f"[3/3] Checking Redis for '{probe_word}'...")

    # Construct the key exactly as it appears in your SCAN output
    redis_key = f"feature:word_count:{probe_word}"

    found_in_redis = False
    count = None

    # Poll Redis a few times to account for the Kafka-to-Redis sink lag
    for _ in range(10):
        # We use .get() because these are Strings, not HASHes
        count = redis_client.get(redis_key)
        if count is not None:
            found_in_redis = True
            break
        time.sleep(1)

    assert found_in_redis, f"FAILED: Key '{redis_key}' not found in Redis. Keys present: {redis_client.keys('feature:word_count:*')}"

    print(f"✅ Success! Redis key '{redis_key}' has value: {count}")

if __name__ == "__main__":
    pytest.main([__file__, "-s"])
