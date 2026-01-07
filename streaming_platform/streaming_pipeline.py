from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes
from pyflink.table.udf import udtf
from pyflink.table.expressions import col
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

import kafka
import time
import os

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPICS = ["raw-sentences", "word-counts"]

# --- 1. Wait for Kafka to be ready ---
def wait_for_kafka(broker: str, timeout=30):
    start = time.time()
    while time.time() - start < timeout:
        try:
            admin = KafkaAdminClient(bootstrap_servers=broker)
            admin.list_topics()  # simple call to check readiness
            print(f"✅ Kafka is ready at {broker}")
            return
        except kafka.errors.NoBrokersAvailable:
            print("Waiting for Kafka...")
            time.sleep(1)
    raise RuntimeError(f"Kafka not ready after {timeout}s")

# --- 2. Create topics if missing ---
from kafka.admin import NewTopic, KafkaAdminClient
from kafka.errors import TopicAlreadyExistsError

def create_topics(broker, topics):
    admin = KafkaAdminClient(bootstrap_servers=broker)
    new_topics = [NewTopic(name=t, num_partitions=1, replication_factor=1) for t in topics]

    try:
        admin.create_topics(new_topics=new_topics)
        print(f"Topics created: {topics}")
    except TopicAlreadyExistsError:
        print("Topics already exist, skipping creation")

# --- 3. UDTF for splitting lines ---
@udtf(result_types=[DataTypes.STRING()])
def split_line(line: str):
    if line:
        for word in line.split():
            yield word

# --- 4. Flink environment setup ---
def setup_environment():
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = TableEnvironment.create(env_settings)
    
    # Determine if running inside Docker
    def running_in_docker() -> bool:
        return (
            os.getenv("FLINK_RUNTIME") == "docker"
            or os.path.exists("/.dockerenv")
        )

    if running_in_docker():
        jar_name = "flink-sql-connector-kafka-3.0.1-1.18.jar"
    else:
        jar_name = "flink-sql-connector-kafka-4.0.0-2.0.jar"

#    jar_name = "flink-sql-connector-kafka-3.0.1-1.18.jar"
    base_dir = os.path.dirname(os.path.abspath(__file__))
    jar_path = os.path.abspath(os.path.join(base_dir, "..", "lib", jar_name))
 
    if os.path.exists(jar_path):
        t_env.get_config().set("pipeline.jars", f"file://{jar_path}")
        print(f"Loaded Kafka connector JAR: {jar_path}")
    else:
        print(f"⚠️ Kafka connector JAR NOT FOUND at {jar_path}")
    
    return t_env

# --- 5. Define tables ---
def define_tables(t_env):
    # Source
    t_env.execute_sql(f"""
        CREATE TABLE kafka_source (
            line STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'raw-sentences',
            'properties.bootstrap.servers' = '{KAFKA_BROKER}',
            'properties.group.id' = 'flink-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'raw'
        )
    """)
    
    # Sink
    t_env.execute_sql(f"""
        CREATE TABLE kafka_sink (
            word STRING PRIMARY KEY NOT ENFORCED,
            cnt BIGINT
        ) WITH (
            'connector' = 'upsert-kafka',
            'topic' = 'word-counts',
            'properties.bootstrap.servers' = '{KAFKA_BROKER}',
            'key.format' = 'json',
            'value.format' = 'json'
        )
    """)

# --- 6. Word count logic ---
def get_word_count_logic(input_table):
    return input_table \
        .flat_map(split_line(col("line"))).alias("word") \
        .group_by(col("word")) \
        .select(col("word"), col("word").count.alias("cnt"))

# --- 7. Run the pipeline ---
def run_pipeline():
    wait_for_kafka(KAFKA_BROKER)
    create_topics(KAFKA_BROKER, TOPICS)
    
    t_env = setup_environment()
    define_tables(t_env)
    
    source_table = t_env.from_path("kafka_source")
    result_table = get_word_count_logic(source_table)
    
    print("Submitting Flink job...")
    table_result = result_table.execute_insert("kafka_sink")
    print(f"Job ID: {table_result.get_job_client().get_job_id()}")
    
    # Keep streaming job running
    table_result.wait()

if __name__ == "__main__":
    run_pipeline()

