"""PostgreSQL resource configuration for Dagster."""

import os
from sqlalchemy import create_engine
from dagster import resource, Field, String, Int


@resource(
    config_schema={
        "host": Field(String, default_value=os.getenv("POSTGRES_HOST", "localhost")),
        "port": Field(Int, default_value=int(os.getenv("POSTGRES_PORT", "5432"))),
        "database": Field(String, default_value=os.getenv("POSTGRES_DB", "ny_taxi")),
        "user": Field(String, default_value=os.getenv("POSTGRES_USER", "root")),
        "password": Field(String, default_value=os.getenv("POSTGRES_PASSWORD", "root")),
    }
)
def postgres_resource(context):
    """Resource that provides a SQLAlchemy engine for PostgreSQL."""
    config = context.resource_config
    connection_string = (
        f"postgresql://{config['user']}:{config['password']}@"
        f"{config['host']}:{config['port']}/{config['database']}"
    )
    engine = create_engine(connection_string)
    return engine
