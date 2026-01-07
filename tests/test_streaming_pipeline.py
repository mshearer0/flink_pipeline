import pytest
import sys
import os
from pyflink.table import EnvironmentSettings, TableEnvironment

# Ensure the parent directory is in the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from streaming_platform.streaming_pipeline import get_word_count_logic

@pytest.fixture(scope="session")
def t_env():
    """Starts the Flink JVM once per test session."""
    settings = EnvironmentSettings.new_instance().in_batch_mode().build()
    env = TableEnvironment.create(settings)
    
    # --- Performance Tuning ---
    # 1. Minimize slots to reduce startup overhead
    env.get_config().set("parallelism.default", "1")
    # 2. Disable heavy lookup/cache services for unit tests
    env.get_config().set("python.fn-execution.bundle.size", "1")
    
    return env

def test_get_word_count_logic(t_env):
    """Verifies the word count transformation."""
    data = [
        ("fraud alert login",),
        ("login success",),
        ("fraud detected",),
        ("",) 
    ]
    
    input_table = t_env.from_elements(data, ["line"])
    result_table = get_word_count_logic(input_table)

    # Convert results to a dictionary
    results = [row for row in result_table.execute().collect()]
    actual_counts = {row[0]: row[1] for row in results}

    assert actual_counts["fraud"] == 2
    assert actual_counts["login"] == 2
    assert "" not in actual_counts
    
    print("\n✅ Logic Unit Test Passed!")

def test_empty_input(t_env):
    """Second test - this will run instantly because t_env is already warm."""
    data = [(" ",)]
    input_table = t_env.from_elements(data, ["line"])
    result_table = get_word_count_logic(input_table)
    
    results = list(result_table.execute().collect())
    assert len(results) == 0
