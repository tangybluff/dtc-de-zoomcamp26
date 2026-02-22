"""Dagster workflows for Data Engineering Zoomcamp."""

# This module is the central registry for all Dagster jobs/schedules in this project.
# Dagster loads `defs` from here to discover what can be launched in the UI.

from dagster import (
    Definitions,
)

# Import all jobs from numbered modules so learning/execution order is explicit.
from .dag_01_hello_world import hello_world_job, hello_world_daily_schedule
from .dag_02_python_tasks import python_stats_job
from .dag_03_data_pipeline import data_pipeline_job
from .dag_04_postgres_taxi import postgres_taxi_ingest_job, postgres_taxi_daily_schedule
from .dag_05_gcp_integration import gcp_setup_job, gcp_data_pipeline_job, gcp_daily_schedule
from .dag_06_chat_pipeline import (
    chat_without_rag_job,
    chat_with_rag_job,
    interactive_chat_session_job
)


# Build a Definitions object that Dagster uses as the project entry point.
defs = Definitions(
    jobs=[
        # Starter learning jobs
        hello_world_job,
        python_stats_job,
        data_pipeline_job,

        # Data engineering ingestion/integration jobs
        postgres_taxi_ingest_job,
        gcp_setup_job,
        gcp_data_pipeline_job,

        # RAG skeleton jobs
        chat_without_rag_job,
        chat_with_rag_job,
        interactive_chat_session_job,
    ],
    schedules=[
        # Enabled schedules are listed here so daemon can trigger them automatically.
        hello_world_daily_schedule,
        postgres_taxi_daily_schedule,
        gcp_daily_schedule,
    ]
)
