# Flink Kafka Redis E2E Pipeline

Fully containerized Flink streaming pipeline with Kafka source and Redis sink.

Run locally:
```bash
docker compose up --build 

check logs with

docker logs -f flink-test-runner-1

Memgraph Tests

echo '{"text": "fraud alert", "user": "Michael"}' | docker exec -i kafka kafka-console-producer --bootstrap-server localhost:9092 --topic speech

docker compose logs -f bridge

echo "MATCH (p:Person) RETURN p.name, p.risk_score;" | docker exec -i memgraph mgconsole

Local tests:

./run_fincrime_platform.sh
python -m pytest ./tests/test_e2e.py

remember to run within venv: source ./bin/activate

