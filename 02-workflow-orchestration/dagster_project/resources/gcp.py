"""Google Cloud Platform resource configuration for Dagster."""

import os
from google.cloud import storage, bigquery
from dagster import resource, Field, String


@resource(
    config_schema={
        "project_id": Field(String, default_value=os.getenv("GCP_PROJECT_ID", "")),
        "credentials_path": Field(String, default_value=os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")),
    }
)
def gcp_resource(context):
    """Resource that provides GCP clients (Storage and BigQuery)."""
    config = context.resource_config
    
    return {
        "storage_client": storage.Client(project=config["project_id"]),
        "bigquery_client": bigquery.Client(project=config["project_id"]),
    }
