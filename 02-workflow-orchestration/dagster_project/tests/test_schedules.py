"""Integration tests for Dagster jobs."""

from dags.dag_01_hello_world import hello_world_daily_schedule
from dags.dag_04_postgres_taxi import postgres_taxi_daily_schedule
from dags.dag_05_gcp_integration import gcp_daily_schedule


def test_hello_world_schedule():
    """Test that the hello world schedule can be evaluated."""
    run_config = hello_world_daily_schedule(None)
    assert run_config == {}


def test_postgres_taxi_schedule():
    """Test that taxi schedule produces default run config."""
    run_config = postgres_taxi_daily_schedule(None)
    assert run_config["taxi"] == "yellow"
    assert len(run_config["year"]) == 4
    assert len(run_config["month"]) == 2


def test_gcp_daily_schedule():
    """Test that GCP schedule returns empty default run config."""
    run_config = gcp_daily_schedule(None)
    assert run_config == {}
