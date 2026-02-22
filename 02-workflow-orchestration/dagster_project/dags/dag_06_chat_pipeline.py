"""DE-useful RAG skeleton: deterministic, observable, and API-key free."""

# This file demonstrates how a data engineer can orchestrate RAG-like stages in Dagster
# without relying on external model APIs: ingest, chunk, index, retrieve, answer, evaluate, persist.

import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import requests
from dagster import job, op


DEFAULT_QUERY = "Which features were released in Kestra 1.1? List at least 5 major features."

DEFAULT_DOCS = [
    {
        "id": "kestra_release_1_1",
        "title": "Kestra 1.1 Release Notes (summary)",
        "source": "embedded",
        "content": (
            "Kestra 1.1 release highlights include better plugin development tooling, improved scheduling ergonomics, "
            "enhanced UI/UX around executions, stronger secret management options, and better Kubernetes deployment "
            "support. Additional improvements include reliability updates, observability enhancements, and docs refresh."
        ),
    },
    {
        "id": "rag_primer",
        "title": "RAG Basics for Data Engineers",
        "source": "embedded",
        "content": (
            "RAG pipelines usually include ingestion, chunking, indexing, retrieval, generation, and evaluation. "
            "Data engineering priorities include lineage, quality checks, retries, idempotency, and artifact persistence."
        ),
    },
]


def _tokenize(text: str) -> List[str]:
    # Normalize text into simple tokens for deterministic lexical scoring.
    return re.findall(r"[a-zA-Z0-9_]+", text.lower())


def _chunk_text(text: str, chunk_size: int = 50, overlap: int = 10) -> List[str]:
    # Sliding-window chunking approximates real embedding chunk preparation.
    tokens = _tokenize(text)
    if not tokens:
        return []
    chunks: List[str] = []
    step = max(1, chunk_size - overlap)
    for start in range(0, len(tokens), step):
        chunk = tokens[start : start + chunk_size]
        if not chunk:
            continue
        chunks.append(" ".join(chunk))
        if start + chunk_size >= len(tokens):
            break
    return chunks


def _artifact_dir(context) -> Path:
    # Each run gets its own artifact directory for reproducibility and auditing.
    base = os.getenv("RAG_ARTIFACTS_DIR", "/app/storage/rag_artifacts")
    run_id = getattr(context, "run_id", "local")
    path = Path(base) / run_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_json(context, name: str, payload: Dict[str, Any]) -> str:
    # Persist operational artifacts so results can be inspected outside Dagster logs.
    destination = _artifact_dir(context) / f"{name}.json"
    with open(destination, "w", encoding="utf-8") as output_file:
        json.dump(payload, output_file, indent=2)
    return str(destination)


@op
def ingest_documents(context) -> List[Dict[str, str]]:
    """Ingest docs from embedded defaults and optional external URL (no API key required)."""
    documents = list(DEFAULT_DOCS)

    # Optional external source lets you test ingestion variability without code changes.
    external_url = os.getenv("RAG_SOURCE_URL", "").strip()
    if external_url:
        context.log.info(f"Attempting external ingestion from {external_url}")
        try:
            response = requests.get(external_url, timeout=20)
            response.raise_for_status()
            documents.append(
                {
                    "id": "external_source",
                    "title": "External Source Document",
                    "source": external_url,
                    "content": response.text,
                }
            )
            context.log.info("External source ingested successfully")
        except Exception as error:
            context.log.warning(f"External ingestion failed; continuing with embedded docs. Reason: {error}")

    context.log.info(f"Ingested {len(documents)} documents")
    return documents


@op
def chunk_documents(context, documents: List[Dict[str, str]]) -> Dict[str, Any]:
    """Chunk documents and produce ingestion metrics."""
    chunks: List[Dict[str, Any]] = []
    # Build chunk records that carry provenance (doc_id/title) for later citation.
    for document in documents:
        for index, chunk in enumerate(_chunk_text(document["content"])):
            chunks.append(
                {
                    "chunk_id": f"{document['id']}_chunk_{index:03d}",
                    "doc_id": document["id"],
                    "title": document["title"],
                    "text": chunk,
                    "token_count": len(_tokenize(chunk)),
                }
            )

    average_tokens = sum(chunk["token_count"] for chunk in chunks) / max(1, len(chunks))
    metrics = {
        "document_count": len(documents),
        "chunk_count": len(chunks),
        "avg_chunk_tokens": round(average_tokens, 2),
        "timestamp": datetime.utcnow().isoformat(),
    }
    context.log.info(f"Chunking metrics: {metrics}")
    return {"chunks": chunks, "metrics": metrics}


@op
def build_mock_index(context, chunk_payload: Dict[str, Any]) -> Dict[str, Any]:
    """Build a deterministic lexical index (stand-in for vector DB)."""
    entries = []
    vocabulary = set()

    # Lexical index here stands in for a vector index and remains fully deterministic.
    for chunk in chunk_payload["chunks"]:
        token_list = _tokenize(chunk["text"])
        vocabulary.update(token_list)
        entries.append(
            {
                "chunk_id": chunk["chunk_id"],
                "doc_id": chunk["doc_id"],
                "title": chunk["title"],
                "text": chunk["text"],
                "tokens": sorted(set(token_list)),
            }
        )

    metrics = {
        "indexed_chunks": len(entries),
        "vocabulary_size": len(vocabulary),
        "index_type": "lexical_mock_index",
        "timestamp": datetime.utcnow().isoformat(),
    }
    context.log.info(f"Index metrics: {metrics}")
    return {"entries": entries, "metrics": metrics}


@op
def build_query(context) -> str:
    """Provide a default question for baseline vs RAG comparison."""
    # Keep query fixed to make before/after comparisons easier.
    context.log.info(f"Query: {DEFAULT_QUERY}")
    return DEFAULT_QUERY


@op
def retrieve_relevant_chunks(context, index_payload: Dict[str, Any], query: str, top_k: int = 3) -> Dict[str, Any]:
    """Retrieve top-k chunks by lexical overlap score."""
    query_tokens = set(_tokenize(query))
    scored: List[Dict[str, Any]] = []

    # Score by token overlap (Jaccard-like) to emulate ranking behavior.
    for entry in index_payload["entries"]:
        entry_tokens = set(entry["tokens"])
        overlap = len(query_tokens.intersection(entry_tokens))
        union = len(query_tokens.union(entry_tokens)) or 1
        score = overlap / union
        scored.append(
            {
                "chunk_id": entry["chunk_id"],
                "doc_id": entry["doc_id"],
                "title": entry["title"],
                "text": entry["text"],
                "score": round(score, 4),
            }
        )

    scored.sort(key=lambda item: item["score"], reverse=True)
    retrieved = scored[:top_k]
    metrics = {
        "top_k": top_k,
        "retrieved_count": len(retrieved),
        "best_score": retrieved[0]["score"] if retrieved else 0,
        "timestamp": datetime.utcnow().isoformat(),
    }
    context.log.info(f"Retrieval metrics: {metrics}")
    return {"query": query, "retrieved": retrieved, "metrics": metrics}


@op
def generate_baseline_answer(context, query: str) -> Dict[str, Any]:
    """Generate a non-RAG baseline answer (generic)."""
    # Baseline intentionally has no citations so you can compare against grounded responses.
    answer = (
        "Without retrieval context, this answer is generic: Kestra 1.1 likely included improvements in UI, "
        "scheduling, plugins, reliability, and documentation."
    )
    context.log.info("Generated baseline non-RAG answer")
    return {
        "query": query,
        "answer": answer,
        "citations": [],
        "mode": "without_rag",
        "timestamp": datetime.utcnow().isoformat(),
    }


@op
def generate_grounded_answer(context, retrieval_payload: Dict[str, Any]) -> Dict[str, Any]:
    """Generate a context-grounded answer from retrieved chunks."""
    query = retrieval_payload["query"]
    retrieved = retrieval_payload["retrieved"]
    bullet_points = []

    # Build answer snippets directly from retrieved chunks as explicit grounding.
    for item in retrieved:
        snippet = item["text"][:140]
        bullet_points.append(f"- [{item['chunk_id']}] {snippet}...")

    answer = (
        f"Using retrieved context for query: {query}\n"
        + "\n".join(bullet_points)
        + "\n\nThis response is grounded in retrieved chunks from the indexed corpus."
    )

    citations = [item["chunk_id"] for item in retrieved]
    context.log.info(f"Generated grounded answer with {len(citations)} citations")
    return {
        "query": query,
        "answer": answer,
        "citations": citations,
        "mode": "with_rag",
        "timestamp": datetime.utcnow().isoformat(),
    }


@op
def evaluate_baseline_answer(context, baseline_answer: Dict[str, Any]) -> Dict[str, Any]:
    """Compute simple baseline quality metrics."""
    answer_tokens = _tokenize(baseline_answer["answer"])
    # Baseline grounding score remains zero by design.
    metrics = {
        "mode": "without_rag",
        "answer_token_count": len(answer_tokens),
        "citation_count": len(baseline_answer["citations"]),
        "grounding_score": 0.0,
        "timestamp": datetime.utcnow().isoformat(),
    }
    context.log.info(f"Baseline metrics: {metrics}")
    return metrics


@op
def evaluate_grounded_answer(context, grounded_answer: Dict[str, Any], retrieval_payload: Dict[str, Any]) -> Dict[str, Any]:
    """Compute simple grounded-answer metrics for observability."""
    retrieved_count = len(retrieval_payload["retrieved"])
    citations = grounded_answer["citations"]
    answer_tokens = _tokenize(grounded_answer["answer"])

    # Simple grounding proxy: fraction of retrieved chunks that are cited in answer.
    grounding_score = len(citations) / max(1, retrieved_count)
    metrics = {
        "mode": "with_rag",
        "answer_token_count": len(answer_tokens),
        "retrieved_count": retrieved_count,
        "citation_count": len(citations),
        "grounding_score": round(grounding_score, 2),
        "timestamp": datetime.utcnow().isoformat(),
    }
    context.log.info(f"Grounded metrics: {metrics}")
    return metrics


@op
def persist_baseline_artifacts(context, baseline_answer: Dict[str, Any], baseline_metrics: Dict[str, Any]) -> Dict[str, str]:
    """Persist baseline run artifacts as JSON files."""
    # Persist answer + metrics bundle for downstream debugging/comparison.
    payload = {
        "answer": baseline_answer,
        "metrics": baseline_metrics,
    }
    artifact_path = _write_json(context, "without_rag_artifacts", payload)
    context.log.info(f"Saved baseline artifacts to {artifact_path}")
    return {"artifact_path": artifact_path}


@op
def persist_rag_artifacts(
    context,
    chunk_payload: Dict[str, Any],
    index_payload: Dict[str, Any],
    retrieval_payload: Dict[str, Any],
    grounded_answer: Dict[str, Any],
    grounded_metrics: Dict[str, Any],
) -> Dict[str, str]:
    """Persist RAG run artifacts as JSON files."""
    # Persist end-to-end evidence of what was indexed, retrieved, and generated.
    payload = {
        "chunk_metrics": chunk_payload["metrics"],
        "index_metrics": index_payload["metrics"],
        "retrieval": retrieval_payload,
        "answer": grounded_answer,
        "metrics": grounded_metrics,
    }
    artifact_path = _write_json(context, "with_rag_artifacts", payload)
    context.log.info(f"Saved RAG artifacts to {artifact_path}")
    return {"artifact_path": artifact_path}


@op
def build_query_batch(context) -> List[str]:
    """Provide a small evaluation batch for interactive session simulation."""
    # Query batch enables lightweight multi-query regression checks.
    queries = [
        DEFAULT_QUERY,
        "What parts of this pipeline are useful for data engineers?",
        "Which steps improve answer grounding in a RAG workflow?",
    ]
    context.log.info(f"Prepared {len(queries)} session queries")
    return queries


@op
def run_session_with_rag(context, index_payload: Dict[str, Any], queries: List[str], top_k: int = 2) -> Dict[str, Any]:
    """Run a multi-query session and produce aggregate metrics."""
    entries = index_payload["entries"]
    session_results = []

    # Process each query independently to surface per-query retrieval behavior.
    for query in queries:
        query_tokens = set(_tokenize(query))
        scored = []
        for entry in entries:
            entry_tokens = set(entry["tokens"])
            overlap = len(query_tokens.intersection(entry_tokens))
            union = len(query_tokens.union(entry_tokens)) or 1
            scored.append({**entry, "score": round(overlap / union, 4)})
        scored.sort(key=lambda item: item["score"], reverse=True)
        retrieved = scored[:top_k]
        answer = "\n".join([f"- [{item['chunk_id']}] {item['text'][:100]}..." for item in retrieved])
        session_results.append(
            {
                "query": query,
                "retrieved_count": len(retrieved),
                "best_score": retrieved[0]["score"] if retrieved else 0,
                "answer": answer,
            }
        )

    # Aggregate metrics provide a compact quality snapshot for the whole batch.
    aggregate = {
        "query_count": len(queries),
        "avg_best_score": round(
            sum(item["best_score"] for item in session_results) / max(1, len(session_results)),
            4,
        ),
        "timestamp": datetime.utcnow().isoformat(),
    }
    context.log.info(f"Session aggregate metrics: {aggregate}")
    return {"results": session_results, "aggregate": aggregate}


@op
def persist_session_artifacts(context, session_payload: Dict[str, Any]) -> Dict[str, str]:
    """Persist interactive session artifacts."""
    # Persist one file per run for easy historical comparisons.
    artifact_path = _write_json(context, "interactive_session_artifacts", session_payload)
    context.log.info(f"Saved session artifacts to {artifact_path}")
    return {"artifact_path": artifact_path}


@job
def chat_without_rag_job():
    """Baseline path: query -> generic answer -> metrics -> artifacts."""
    # This job represents "LLM without retrieval context".
    query = build_query()
    baseline_answer = generate_baseline_answer(query)
    baseline_metrics = evaluate_baseline_answer(baseline_answer)
    persist_baseline_artifacts(baseline_answer, baseline_metrics)


@job
def chat_with_rag_job():
    """RAG path: ingest -> chunk -> index -> retrieve -> grounded answer -> metrics -> artifacts."""
    # This job is the DE-focused RAG orchestration baseline.
    documents = ingest_documents()
    chunk_payload = chunk_documents(documents)
    index_payload = build_mock_index(chunk_payload)
    query = build_query()
    retrieval_payload = retrieve_relevant_chunks(index_payload, query)
    grounded_answer = generate_grounded_answer(retrieval_payload)
    grounded_metrics = evaluate_grounded_answer(grounded_answer, retrieval_payload)
    persist_rag_artifacts(
        chunk_payload,
        index_payload,
        retrieval_payload,
        grounded_answer,
        grounded_metrics,
    )


@job
def interactive_chat_session_job():
    """Batch RAG simulation for multiple queries with aggregate metrics."""
    # This job is useful for periodic QA/regression style checks.
    documents = ingest_documents()
    chunk_payload = chunk_documents(documents)
    index_payload = build_mock_index(chunk_payload)
    queries = build_query_batch()
    session_payload = run_session_with_rag(index_payload, queries)
    persist_session_artifacts(session_payload)
