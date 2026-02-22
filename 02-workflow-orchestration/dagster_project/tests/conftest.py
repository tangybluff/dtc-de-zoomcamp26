"""Test configuration examples for running jobs locally."""

import os
import sys
from pathlib import Path

# Ensure tests can import project modules in both local and container executions.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = PROJECT_ROOT / "dags"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

# Example run config for hello world job
HELLO_WORLD_RUN_CONFIG = {
    "ops": {
        "hello_message": {
            "config": {}
        }
    }
}

# Example run config for postgres taxi job
POSTGRES_TAXI_RUN_CONFIG = {
    "ops": {
        "extract_taxi_data": {
            "inputs": {
                "taxi": "yellow",
                "year": "2019",
                "month": "01"
            }
        }
    }
}

# Example run config for GCP job
GCP_RUN_CONFIG = {
    "ops": {
        "setup_gcp_credentials": {
            "config": {
                "project_id": os.getenv("GCP_PROJECT_ID", "my-project"),
            }
        }
    }
}


def test_run_config_schema():
    """Test that run configs are valid dictionaries."""
    assert isinstance(HELLO_WORLD_RUN_CONFIG, dict)
    assert isinstance(POSTGRES_TAXI_RUN_CONFIG, dict)
    assert isinstance(GCP_RUN_CONFIG, dict)
