import json
from kafka import KafkaConsumer
import redis
from datetime import datetime

import os

KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')


# 1. Setup Redis Connection
r = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)

# 2. Setup Kafka Consumer
consumer = KafkaConsumer(
    'word-counts',
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='redis-feature-sink-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    key_deserializer=lambda m: m.decode('utf-8') if m else None
)

print("--- Redis Feature Store Sink Active ---")

try:
    for message in consumer:
        data = message.value
        word = data.get('word')
        count = data.get('cnt')

        if word:
            redis_key = f"feature:word_count:{word}"
            r.set(redis_key, count)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Updated Redis -> {word}: {count}")

except KeyboardInterrupt:
    print("Stopping Sink...")
finally:
    consumer.close()
