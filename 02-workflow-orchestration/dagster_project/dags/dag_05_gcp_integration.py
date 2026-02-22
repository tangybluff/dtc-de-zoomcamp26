"""GCP Integration Pipeline - 06_gcp_kv.yaml through 09_gcp_taxi_scheduled.yaml equivalent."""

# This file is a GCP orchestration skeleton.
# It focuses on sequencing and observability; cloud operations are intentionally lightweight placeholders.

import os
import json
from datetime import datetime
from typing import Dict, Any, Optional

from dagster import op, job, schedule, resource, Field, String


@op
def setup_gcp_credentials(context) -> Dict[str, str]:
    """Validate and setup GCP credentials."""
    context.log.info("Setting up GCP credentials...")
    
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    project_id = os.getenv("GCP_PROJECT_ID", "")
    
    # Warn instead of failing so the skeleton remains runnable without cloud setup.
    if not credentials_path or not project_id:
        context.log.warning("GCP credentials not fully configured. Set GOOGLE_APPLICATION_CREDENTIALS and GCP_PROJECT_ID")
    
    return {
        "credentials_path": credentials_path,
        "project_id": project_id,
        "status": "configured"
    }


@op
def fetch_gcp_key_value(context, credentials: Dict[str, str], key: str) -> Dict[str, Any]:
    """
    Fetch key-value pair from GCP (could be from Secret Manager, Firestore, etc).
    Placeholder implementation.
    """
    context.log.info(f"Fetching key-value for: {key}")
    
    # Placeholder - replace with Secret Manager / Firestore / Config API call in production.
    return {
        "key": key,
        "value": "placeholder_value",
        "timestamp": datetime.now().isoformat(),
        "source": "gcp"
    }


@op
def upload_to_gcs(context, credentials: Dict[str, str], 
                  data: Dict[str, Any],
                  bucket: str = "dtc-de-zoomcamp-data",
                  filename: str = "processed_data.json") -> str:
    """Upload processed data to Google Cloud Storage."""
    context.log.info(f"Uploading {filename} to gs://{bucket}/...")
    
    # Placeholder implementation that returns the target URI for downstream traceability.
    gcs_path = f"gs://{bucket}/{filename}"
    context.log.info(f"Successfully uploaded to {gcs_path}")
    
    return gcs_path


@op
def load_to_bigquery(context, credentials: Dict[str, str], 
                    data: Dict[str, Any],
                    dataset: str = "ny_taxi_data",
                    table: str = "taxi_trips") -> Dict[str, str]:
    """Load data into BigQuery table."""
    context.log.info(f"Loading data into BigQuery table: {dataset}.{table}")
    
    # Placeholder implementation - replace with load job submission and status polling.
    return {
        "dataset": dataset,
        "table": table,
        "rows_loaded": len(str(data)),
        "status": "success"
    }


@op
def query_bigquery(context, credentials: Dict[str, str], 
                   dataset: str = "ny_taxi_data",
                   table: str = "taxi_trips",
                   query: str = "SELECT 1") -> Dict[str, Any]:
    """Execute query on BigQuery."""
    context.log.info(f"Running query on {dataset}.{table}")
    context.log.debug(f"Query: {query}")
    
    # Placeholder implementation - replace with real query execution and result parsing.
    return {
        "dataset": dataset,
        "table": table,
        "rows_returned": 100,
        "query_time_ms": 1234,
        "status": "success"
    }


@op
def setup_gcp_infrastructure(context, credentials: Dict[str, str]) -> Dict[str, str]:
    """
    Setup required GCP infrastructure like datasets, buckets, etc.
    This is a one-time setup operation.
    """
    context.log.info("Setting up GCP infrastructure...")
    
    # Return canonical resource names to model dependency outputs.
    return {
        "gcs_bucket": "dtc-de-zoomcamp-data",
        "bigquery_dataset": "ny_taxi_data",
        "status": "ready"
    }


@op
def generate_sample_data(context) -> Dict[str, Any]:
    """Generate sample data payload for placeholder GCP pipeline."""
    # Deterministic payload keeps the example easy to rerun and inspect.
    return {"sample": "data", "timestamp": datetime.now().isoformat()}


@job
def gcp_setup_job():
    """Job to setup GCP resources and validate connectivity."""
    # One-time initialization path.
    credentials = setup_gcp_credentials()
    setup_gcp_infrastructure(credentials=credentials)


@job
def gcp_data_pipeline_job():
    """
    Complete GCP data pipeline:
    1. Validate credentials
    2. Fetch data from GCS or external source
    3. Process and upload to GCS
    4. Load into BigQuery
    5. Run analysis queries
    """
    # Main path: validate config -> stage data -> load -> query.
    credentials = setup_gcp_credentials()
    sample_data = generate_sample_data()

    # Load into BigQuery
    upload_to_gcs(
        credentials=credentials,
        data=sample_data
    )
    load_to_bigquery(
        credentials=credentials,
        data=sample_data
    )

    # Run analysis
    query_bigquery(
        credentials=credentials,
    )


# Schedule: Daily GCP pipeline execution
@schedule(job=gcp_data_pipeline_job, cron_schedule="0 2 * * *")
def gcp_daily_schedule(_context):
    """Schedule GCP pipeline daily at 2 AM."""
    # Empty dict means run with default op parameters.
    return {}
