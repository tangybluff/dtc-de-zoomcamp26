"""Data pipeline - 03_getting_started_data_pipeline.yaml equivalent."""

# This file is a simple ETL skeleton to teach data-flow between steps.
# It uses placeholder data but mirrors real pipeline phases: extract, validate, transform, load.

from dagster import op, job, Out, DynamicOut, DynamicOutput


@op
def extract_data(context):
    """Extract data from source."""
    context.log.info("Starting data extraction...")
    # Placeholder for actual data extraction logic (API call, file read, DB query, etc.).
    sample_data = {
        'rows': 1000,
        'columns': ['id', 'name', 'value'],
        'source': 'external_api'
    }
    context.log.info(f"Extracted {sample_data['rows']} rows")
    return sample_data


@op
def validate_data(context, data):
    """Validate extracted data for quality and completeness."""
    context.log.info(f"Validating {data['rows']} rows...")
    
    # Lightweight quality checks to show where data contracts/expectations fit in the flow.
    validation_results = {
        'valid_rows': int(data['rows'] * 0.99),
        'invalid_rows': int(data['rows'] * 0.01),
        'quality_score': 0.99
    }
    
    if validation_results['quality_score'] > 0.95:
        context.log.info("Data validation passed!")
        return validation_results
    else:
        # Fail fast when quality is below threshold.
        raise Exception("Data quality below threshold")


@op
def transform_data(context, validation_results):
    """Transform and enrich the data."""
    context.log.info(f"Transforming {validation_results['valid_rows']} valid rows...")
    
    # This section represents business transformations before load.
    transformed = {
        'processed_rows': validation_results['valid_rows'],
        'transformations_applied': [
            'null_handling',
            'type_conversion',
            'deduplication'
        ],
        'status': 'transformed'
    }
    
    context.log.info(f"Transformation complete: {transformed}")
    return transformed


@op
def load_data(context, transformed_data):
    """Load transformed data to destination."""
    context.log.info(f"Loading {transformed_data['processed_rows']} rows to data warehouse...")
    
    # In production this would execute a real write (warehouse table, object store, etc.).
    load_result = {
        'rows_loaded': transformed_data['processed_rows'],
        'destination': 'postgres_staging',
        'timestamp': '2024-01-01T00:00:00Z',
        'status': 'success'
    }
    
    context.log.info(f"Data load complete: {load_result}")
    return load_result


@job
def data_pipeline_job():
    """ETL pipeline: Extract → Validate → Transform → Load."""
    # Explicit variable chaining makes dependencies and execution order easy to follow.
    data = extract_data()
    validated = validate_data(data)
    transformed = transform_data(validated)
    load_data(transformed)
