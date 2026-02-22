"""Tests for Dagster jobs and ops."""

import json

import pandas as pd
import pytest
from dagster import Failure, build_op_context
from sqlalchemy import text

from dags.dag_01_hello_world import goodbye_message, generate_output, hello_message
from dags.dag_02_python_tasks import collect_stats
from dags.dag_03_data_pipeline import extract_data, validate_data
from dags.dag_04_postgres_taxi import (
    _build_postgres_engine,
    add_unique_id_and_filename,
    cleanup_files,
    create_taxi_tables,
    load_csv_to_staging,
    merge_data_to_main_table,
)
from dags.dag_06_chat_pipeline import (
    build_mock_index,
    chunk_documents,
    evaluate_grounded_answer,
    generate_grounded_answer,
    persist_rag_artifacts,
    retrieve_relevant_chunks,
)


def test_hello_message():
    """Test the hello message op."""
    context = build_op_context()
    result = hello_message(context)
    assert result == "Hello, Will!"


def test_generate_output():
    """Test the output generation op."""
    context = build_op_context()
    result = generate_output(context, "Hello, Will!")
    assert isinstance(result, dict)
    assert "value" in result


def test_goodbye_message():
    """Test the goodbye message op."""
    context = build_op_context()
    result = goodbye_message(context, {"value": "x", "greeting": "Hello, Will!"})
    assert result == "Goodbye, Will!"


def test_collect_stats_success(monkeypatch):
    """Test GitHub stats op with mocked successful API response."""

    class MockResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "stargazers_count": 123,
                "forks_count": 45,
                "last_updated": "2026-01-01T00:00:00Z",
            }

    monkeypatch.setattr("dags.dag_02_python_tasks.requests.get", lambda _url: MockResponse())

    context = build_op_context()
    result = collect_stats(context)

    assert result["status"] == "success"
    assert result["repository"] == "dagster-io/dagster"
    assert result["stars"] == 123
    assert result["forks"] == 45


def test_collect_stats_failure(monkeypatch):
    """Test GitHub stats op raises Dagster Failure on request errors."""

    def _raise_error(_url):
        raise RuntimeError("network unavailable")

    monkeypatch.setattr("dags.dag_02_python_tasks.requests.get", _raise_error)

    context = build_op_context()
    with pytest.raises(Failure):
        collect_stats(context)


def test_extract_data():
    """Test data extraction op."""
    context = build_op_context()
    result = extract_data(context)
    assert isinstance(result, dict)
    assert "rows" in result
    assert "columns" in result


def test_validate_data_success():
    """Test data validation with valid data."""
    context = build_op_context()
    
    data = {
        'rows': 1000,
        'columns': ['id', 'name', 'value'],
        'source': 'test'
    }
    
    result = validate_data(context, data)
    assert result['quality_score'] == 0.99
    assert result['valid_rows'] > 0


def test_validate_data_failure():
    """Test data validation with invalid data."""
    context = build_op_context()
    
    data = {
        'rows': 0,
        'columns': [],
        'source': 'test'
    }
    
    result = validate_data(context, data)
    assert isinstance(result, dict)
    assert result['valid_rows'] == 0
    assert result['invalid_rows'] == 0
    assert result['quality_score'] == 0.99


def test_rag_chunk_retrieve_and_evaluate_flow():
    """Test deterministic RAG skeleton flow (chunk -> index -> retrieve -> evaluate)."""
    context = build_op_context()
    documents = [
        {
            "id": "doc_1",
            "title": "Test Doc",
            "source": "test",
            "content": "Kestra 1.1 improved plugins scheduling ui reliability and observability.",
        }
    ]

    chunk_payload = chunk_documents(context, documents)
    assert chunk_payload["metrics"]["document_count"] == 1
    assert chunk_payload["metrics"]["chunk_count"] >= 1

    index_payload = build_mock_index(context, chunk_payload)
    retrieval_payload = retrieve_relevant_chunks(context, index_payload, "What improved in Kestra 1.1?")
    assert retrieval_payload["metrics"]["retrieved_count"] >= 1

    grounded_answer = generate_grounded_answer(context, retrieval_payload)
    assert grounded_answer["mode"] == "with_rag"
    assert isinstance(grounded_answer["citations"], list)

    grounded_metrics = evaluate_grounded_answer(context, grounded_answer, retrieval_payload)
    assert grounded_metrics["mode"] == "with_rag"
    assert grounded_metrics["citation_count"] == len(grounded_answer["citations"])


def test_taxi_postgres_integration_lite(tmp_path):
    """Integration-lite test: create tables, load tiny CSV, enrich IDs, and merge to main table."""
    context = build_op_context()

    sample_rows = [
        {
            "VendorID": "1",
            "tpep_pickup_datetime": "2019-01-01 00:00:00",
            "tpep_dropoff_datetime": "2019-01-01 00:10:00",
            "passenger_count": 1,
            "trip_distance": 1.2,
            "RatecodeID": "1",
            "store_and_fwd_flag": "N",
            "PULocationID": "100",
            "DOLocationID": "200",
            "payment_type": 1,
            "fare_amount": 10.5,
            "extra": 0.5,
            "mta_tax": 0.5,
            "tip_amount": 2.0,
            "tolls_amount": 0.0,
            "improvement_surcharge": 0.3,
            "total_amount": 13.8,
            "congestion_surcharge": 2.5,
        },
        {
            "VendorID": "2",
            "tpep_pickup_datetime": "2019-01-01 01:00:00",
            "tpep_dropoff_datetime": "2019-01-01 01:20:00",
            "passenger_count": 2,
            "trip_distance": 3.4,
            "RatecodeID": "1",
            "store_and_fwd_flag": "N",
            "PULocationID": "101",
            "DOLocationID": "201",
            "payment_type": 2,
            "fare_amount": 16.0,
            "extra": 0.5,
            "mta_tax": 0.5,
            "tip_amount": 0.0,
            "tolls_amount": 0.0,
            "improvement_surcharge": 0.3,
            "total_amount": 19.3,
            "congestion_surcharge": 2.5,
        },
    ]

    csv_path = tmp_path / "yellow_tripdata_test.csv"
    pd.DataFrame(sample_rows).to_csv(csv_path, index=False)

    table_info = create_taxi_tables(context, "yellow")
    rows_loaded = load_csv_to_staging(context, str(csv_path), table_info)
    assert rows_loaded == 2

    merge_info = add_unique_id_and_filename(context, rows_loaded, table_info, str(csv_path))
    merge_result = merge_data_to_main_table(context, merge_info)
    assert merge_result["status"] == "success"

    engine = _build_postgres_engine()
    with engine.connect() as connection:
        staged_count = connection.execute(text("SELECT COUNT(*) FROM public.yellow_tripdata_staging")).scalar()
        staged_tagged_count = connection.execute(
            text(
                "SELECT COUNT(*) FROM public.yellow_tripdata_staging "
                "WHERE filename = :filename AND unique_row_id IS NOT NULL"
            ),
            {"filename": str(csv_path)},
        ).scalar()
        main_total_count = connection.execute(
            text("SELECT COUNT(*) FROM public.yellow_tripdata")
        ).scalar()

    assert merge_result["rows_merged"] == 2
    assert staged_count == 2
    assert staged_tagged_count == 2
    assert main_total_count >= 1

    cleanup_files(context, str(csv_path))
    assert not csv_path.exists()


def test_rag_artifact_contract(tmp_path, monkeypatch):
    """Contract test: persisted RAG artifact must include expected top-level sections."""
    monkeypatch.setenv("RAG_ARTIFACTS_DIR", str(tmp_path))
    context = build_op_context()

    documents = [
        {
            "id": "doc_contract",
            "title": "Contract Doc",
            "source": "test",
            "content": "RAG includes ingestion chunking indexing retrieval generation and evaluation.",
        }
    ]

    chunk_payload = chunk_documents(context, documents)
    index_payload = build_mock_index(context, chunk_payload)
    retrieval_payload = retrieve_relevant_chunks(context, index_payload, "What are RAG steps?")
    grounded_answer = generate_grounded_answer(context, retrieval_payload)
    grounded_metrics = evaluate_grounded_answer(context, grounded_answer, retrieval_payload)

    result = persist_rag_artifacts(
        context,
        chunk_payload,
        index_payload,
        retrieval_payload,
        grounded_answer,
        grounded_metrics,
    )

    artifact_path = result["artifact_path"]
    with open(artifact_path, "r", encoding="utf-8") as artifact_file:
        payload = json.load(artifact_file)

    assert set(payload.keys()) == {"chunk_metrics", "index_metrics", "retrieval", "answer", "metrics"}
    assert payload["retrieval"]["query"]
    assert isinstance(payload["answer"]["citations"], list)
    assert payload["metrics"]["mode"] == "with_rag"
