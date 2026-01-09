import json
from confluent_kafka import Consumer, KafkaError
from neo4j import GraphDatabase

# --- CONFIGURATION ---

KAFKA_CONF = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'memgraph-bridge-group',
    'auto.offset.reset': 'earliest'
}
MEMGRAPH_URI = "bolt://memgraph:7687"

# --- NEO4J / MEMGRAPH DRIVER ---
driver = GraphDatabase.driver(MEMGRAPH_URI, auth=("", ""))

def initialize_graph():
    """Sets up indexes and constraints for performance."""
    with driver.session() as session:
        # Ensures we don't have duplicate Persons and can find them instantly
        session.run("CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE;")
        session.run("CREATE INDEX ON :Speech(text);")
        print("Graph constraints and indexes initialized.")

def process_messages():
    consumer = Consumer(KAFKA_CONF)
    consumer.subscribe(['speech'])

    # The Logic: Link Person to Speech and initialize risk fields
    cypher_query = """
    MERGE (p:Person {name: $user})
    ON CREATE SET p.risk_score = 0.0
    CREATE (s:Speech {
        text: $text, 
        created_at: datetime(),
        risk_score: 0.0,
        flagged: false
    })
    MERGE (p)-[:SPOKE]->(s)
    RETURN p.name, s.text
    """

    print("Bridge started. Waiting for Kafka messages...")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            try:
                # Decode and Parse
                data = json.loads(msg.value().decode('utf-8'))
                user = data.get('user', 'Unknown')
                text = data.get('text', '')

                # Push to Memgraph
                with driver.session() as session:
                    session.run(cypher_query, user=user, text=text)
                    print(f"Processed: {user} -> {text[:30]}...")

            except Exception as e:
                print(f"Logic Error: {e}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        driver.close()

if __name__ == "__main__":
    initialize_graph()
    process_messages()
