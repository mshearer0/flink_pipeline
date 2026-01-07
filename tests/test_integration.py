import pytest
import sys
import os
from pyflink.table import EnvironmentSettings, TableEnvironment

# Ensure imports work
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from streaming_platform.streaming_pipeline import get_word_count_logic

@pytest.fixture(scope="module")
def t_env():
    """Shared TableEnvironment for integration tests."""
    settings = EnvironmentSettings.new_instance().in_batch_mode().build()
    env = TableEnvironment.create(settings)
    env.get_config().set("parallelism.default", "1")
    return env

def test_pipeline_integration(t_env):
    """Tests the full bridge from mock source to result collection."""
    
    # 1. Simulate Kafka Input
    input_data = [
        ("transaction_limit_reached",),
        ("transaction_limit_reached",),
        ("login_failure",)
    ]
    source_table = t_env.from_elements(input_data, ["line"])

    # 2. Run through the logic function
    # This checks if the Table API transformations handle the schema correctly
    result_table = get_word_count_logic(source_table)

    # 3. Simulate the Sink Bridge
    # In a real run, this goes to Kafka/Redis. Here we verify the 'collect' bridge.
    results = [row for row in result_table.execute().collect()]
    
    # Map for easy validation
    final_view = {row[0]: row[1] for row in results}

    # 4. Integration Assertions
    assert "transaction_limit_reached" in final_view
    assert final_view["transaction_limit_reached"] == 2
    assert final_view["login_failure"] == 1
    
    print("\n✅ Integration Bridge Verified: Data flows correctly through the schema.")
