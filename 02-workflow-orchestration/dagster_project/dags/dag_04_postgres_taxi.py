"""PostgreSQL Taxi Data Pipeline - 04_postgres_taxi.yaml and 05_postgres_taxi_scheduled.yaml equivalent."""

# This file is a practical DE ingestion pipeline:
# download raw monthly taxi data, stage it, enrich it with IDs, upsert to final table, then clean up files.

import os
import subprocess
import pandas as pd
from datetime import datetime, timedelta
import gzip
import urllib.request
from typing import Dict, Any

from dagster import (
    op, job, schedule, DagsterInvariantViolationError,
    resource, Field, String, In, Out, DynamicOut, DynamicOutput, Noneable
)
from sqlalchemy import text, create_engine
from sqlalchemy.engine import Engine


# Taxi schema definitions
YELLOW_TAXI_SCHEMA = {
    'unique_row_id': 'text',
    'filename': 'text',
    'VendorID': 'text',
    'tpep_pickup_datetime': 'timestamp',
    'tpep_dropoff_datetime': 'timestamp',
    'passenger_count': 'integer',
    'trip_distance': 'double precision',
    'RatecodeID': 'text',
    'store_and_fwd_flag': 'text',
    'PULocationID': 'text',
    'DOLocationID': 'text',
    'payment_type': 'integer',
    'fare_amount': 'double precision',
    'extra': 'double precision',
    'mta_tax': 'double precision',
    'tip_amount': 'double precision',
    'tolls_amount': 'double precision',
    'improvement_surcharge': 'double precision',
    'total_amount': 'double precision',
    'congestion_surcharge': 'double precision',
}

GREEN_TAXI_SCHEMA = {
    'unique_row_id': 'text',
    'filename': 'text',
    'VendorID': 'text',
    'lpep_pickup_datetime': 'timestamp',
    'lpep_dropoff_datetime': 'timestamp',
    'store_and_fwd_flag': 'text',
    'RatecodeID': 'text',
    'PULocationID': 'text',
    'DOLocationID': 'text',
    'passenger_count': 'integer',
    'trip_distance': 'double precision',
    'fare_amount': 'double precision',
    'extra': 'double precision',
    'mta_tax': 'double precision',
    'tip_amount': 'double precision',
    'tolls_amount': 'double precision',
    'ehail_fee': 'double precision',
    'improvement_surcharge': 'double precision',
    'total_amount': 'double precision',
    'payment_type': 'integer',
    'trip_type': 'integer',
    'congestion_surcharge': 'double precision',
}

YELLOW_COLUMNS = [
    'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
    'passenger_count', 'trip_distance', 'RatecodeID', 'store_and_fwd_flag',
    'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount', 'extra',
    'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge',
    'total_amount', 'congestion_surcharge'
]

GREEN_COLUMNS = [
    'VendorID', 'lpep_pickup_datetime', 'lpep_dropoff_datetime',
    'store_and_fwd_flag', 'RatecodeID', 'PULocationID', 'DOLocationID',
    'passenger_count', 'trip_distance', 'fare_amount', 'extra', 'mta_tax',
    'tip_amount', 'tolls_amount', 'ehail_fee', 'improvement_surcharge',
    'total_amount', 'payment_type', 'trip_type', 'congestion_surcharge'
]


def _build_postgres_engine() -> Engine:
    # Centralized DB connection builder so every op resolves the same environment-based credentials.
    host = os.getenv("POSTGRES_HOST", "pgdatabase")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "ny_taxi")
    user = os.getenv("POSTGRES_USER", "root")
    password = os.getenv("POSTGRES_PASSWORD", "root")
    return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")


@op
def extract_taxi_data(context, taxi: str, year: str, month: str) -> str:
    """Download taxi data from GitHub releases."""
    filename = f"{taxi}_tripdata_{year}-{month}.csv"
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi}/{filename}.gz"
    
    context.log.info(f"Extracting {filename} from {url}...")
    
    try:
        # Download gzip stream and write decompressed CSV to local workspace.
        with urllib.request.urlopen(url) as response:
            with gzip.GzipFile(fileobj=response) as gz:
                with open(filename, 'wb') as f:
                    f.write(gz.read())
        
        context.log.info(f"Successfully extracted {filename}")
        return filename
    except Exception as e:
        context.log.error(f"Failed to extract data: {str(e)}")
        raise


@op
def create_taxi_tables(context, taxi: str) -> Dict[str, str]:
    """Create main and staging tables for taxi data."""
    schema = YELLOW_TAXI_SCHEMA if taxi == 'yellow' else GREEN_TAXI_SCHEMA
    table_name = f"public.{taxi}_tripdata"
    staging_table_name = f"public.{taxi}_tripdata_staging"
    
    context.log.info(f"Creating tables for {taxi} taxi data...")
    engine = _build_postgres_engine()
    
    # Build CREATE TABLE statements from selected schema.
    columns = ', '.join([f"{col} {dtype}" for col, dtype in schema.items()])
    
    create_main_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns},
            PRIMARY KEY (unique_row_id)
        );
    """
    
    create_staging_sql = f"""
        CREATE TABLE IF NOT EXISTS {staging_table_name} (
            {columns}
        );
    """
    
    try:
        with engine.connect() as conn:
            conn.execute(text(create_main_sql))
            conn.execute(text(create_staging_sql))
            conn.commit()
        
        context.log.info(f"Successfully created tables for {taxi} taxi")
        return {
            'main_table': table_name,
            'staging_table': staging_table_name,
            'taxi': taxi
        }
    except Exception as e:
        context.log.error(f"Failed to create tables: {str(e)}")
        raise


@op
def load_csv_to_staging(context, filename: str, table_info: Dict[str, str]) -> int:
    """Load CSV data into staging table."""
    staging_table = table_info['staging_table']
    taxi = table_info['taxi']
    chunk_size = int(os.getenv("CSV_CHUNK_SIZE", "50000"))
    
    context.log.info(f"Loading {filename} into {staging_table}...")
    engine = _build_postgres_engine()
    
    try:
        # Truncate staging table to keep each run idempotent at staging level.
        with engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {staging_table};"))
            conn.commit()
        
        # Stream the CSV in chunks to avoid high memory usage for larger files.
        rows_loaded = 0
        for batch_number, chunk_df in enumerate(pd.read_csv(filename, chunksize=chunk_size), start=1):
            # PostgreSQL folds unquoted identifiers to lowercase; normalize CSV headers for consistent inserts.
            chunk_df.columns = [column_name.lower() for column_name in chunk_df.columns]
            chunk_df.to_sql(
                staging_table.split('.')[-1],
                engine,
                schema='public',
                if_exists='append',
                index=False,
            )
            rows_loaded += len(chunk_df)
            context.log.info(
                f"Loaded batch {batch_number} with {len(chunk_df)} rows "
                f"(running total: {rows_loaded})"
            )

        context.log.info(f"Successfully loaded {rows_loaded} rows into {staging_table}")
        return rows_loaded
    except Exception as e:
        context.log.error(f"Failed to load data: {str(e)}")
        raise


@op
def add_unique_id_and_filename(context, num_rows: int, table_info: Dict[str, str], 
                                filename: str) -> Dict[str, Any]:
    """Add unique ID and filename to staging table."""
    staging_table = table_info['staging_table']
    taxi = table_info['taxi']
    
    context.log.info(f"Adding unique IDs and filename to {staging_table}...")
    engine = _build_postgres_engine()
    
    # Determine datetime columns based on taxi dataset type.
    if taxi == 'yellow':
        datetime_cols = "COALESCE(CAST(tpep_pickup_datetime AS text), '') || COALESCE(CAST(tpep_dropoff_datetime AS text), '')"
    else:  # green
        datetime_cols = "COALESCE(CAST(lpep_pickup_datetime AS text), '') || COALESCE(CAST(lpep_dropoff_datetime AS text), '')"
    
    update_sql = f"""
        UPDATE {staging_table}
        SET 
            unique_row_id = md5(
                COALESCE(CAST(VendorID AS text), '') ||
                {datetime_cols} || 
                COALESCE(PULocationID, '') || 
                COALESCE(DOLocationID, '') || 
                COALESCE(CAST(fare_amount AS text), '') || 
                COALESCE(CAST(trip_distance AS text), '')      
            ),
            filename = :filename;
    """
    
    try:
        with engine.connect() as conn:
            conn.execute(text(update_sql), {'filename': filename})
            conn.commit()
        
        context.log.info(f"Successfully updated {num_rows} rows with unique IDs")
        return {
            'rows_processed': num_rows,
            'staging_table': staging_table,
            'main_table': table_info['main_table']
        }
    except Exception as e:
        context.log.error(f"Failed to add unique IDs: {str(e)}")
        raise


@op
def merge_data_to_main_table(context, merge_info: Dict[str, Any]) -> Dict[str, Any]:
    """Merge staged data into main table using UPSERT logic."""
    main_table = merge_info['main_table']
    staging_table = merge_info['staging_table']
    rows_processed = merge_info['rows_processed']
    
    context.log.info(f"Merging {rows_processed} rows from {staging_table} to {main_table}...")
    engine = _build_postgres_engine()
    
    # PostgreSQL upsert pattern: insert all staged rows and skip duplicates by unique_row_id.
    merge_sql = f"""
        INSERT INTO {main_table}
        SELECT * FROM {staging_table}
        ON CONFLICT (unique_row_id) DO NOTHING;
    """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(merge_sql))
            conn.commit()
        
        context.log.info(f"Successfully merged data into {main_table}")
        return {
            'status': 'success',
            'rows_merged': rows_processed,
            'table': main_table,
            'timestamp': datetime.now().isoformat()
        }
    except Exception as e:
        context.log.error(f"Failed to merge data: {str(e)}")
        raise


@op
def cleanup_files(context, filename: str) -> None:
    """Clean up downloaded CSV files."""
    try:
        # Keep local workspace tidy after successful load/merge.
        if os.path.exists(filename):
            os.remove(filename)
            context.log.info(f"Cleaned up {filename}")
    except Exception as e:
        context.log.error(f"Error cleaning up files: {str(e)}")


@job
def postgres_taxi_ingest_job(
    taxi: str = "yellow",
    year: str = "2019",
    month: str = "01"
):
    """
    Full ETL pipeline for NYC taxi data:
    1. Extract CSV from GitHub
    2. Create main and staging tables
    3. Load data into staging
    4. Add unique IDs
    5. Merge into main table
    6. Cleanup files
    """
    # Orchestration flow: extract -> stage -> enrich -> merge -> cleanup.
    filename = extract_taxi_data(taxi=taxi, year=year, month=month)
    table_info = create_taxi_tables(taxi=taxi)
    num_rows = load_csv_to_staging(filename=filename, table_info=table_info)
    merge_info = add_unique_id_and_filename(
        num_rows=num_rows, 
        table_info=table_info, 
        filename=filename
    )
    result = merge_data_to_main_table(merge_info=merge_info)
    cleanup_files(filename=filename)


# Schedule: Daily at 10 AM
@schedule(job=postgres_taxi_ingest_job, cron_schedule="0 10 * * *")
def postgres_taxi_daily_schedule(_context):
    """Schedule taxi data ingestion daily with yellow taxi for this month."""
    # Schedule payload computes current year/month at trigger time.
    current_date = datetime.now()
    return {
        "taxi": "yellow",
        "year": str(current_date.year),
        "month": f"{current_date.month:02d}"
    }
