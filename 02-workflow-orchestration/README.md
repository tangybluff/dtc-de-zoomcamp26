# Workflow Orchestration with Dagster

Welcome to Module 2 of the Data Engineering Zoomcamp! This module covers workflow orchestration using **Dagster**, a modern, Python-based orchestration platform.

Dagster is an asset-oriented orchestrator that emphasizes:
- **Python-first**: Write orchestration logic in pure Python (no YAML configs needed)
- **Type safety**: Full IDE support with type hints and autocomplete
- **Asset lineage**: Track data assets and their dependencies
- **Observable and testable**: Built-in logging, monitoring, and testing utilities

---

## Table of Contents

1. [Overview](#overview)
2. [Quick Start](#quick-start)
3. [Project Structure](#project-structure)
4. [Available Pipelines](#available-pipelines)
5. [Running Locally](#running-locally)
6. [Running with Docker](#running-with-docker)
7. [Configuration](#configuration)
8. [Troubleshooting](#troubleshooting)

---

## Overview

This folder contains Dagster-based orchestration workflows written in Python with job, op, and schedule definitions.

This module is designed to help you think like a data engineer using Dagster:
- model data workflows as explicit, testable steps
- run them repeatedly with clear logs and run metadata
- evolve from "toy" jobs to production-shaped ingestion and quality patterns
- treat AI/RAG pipelines as data pipelines (ingestion, indexing, retrieval, evaluation), not as opaque black boxes

### Dagster-first Concepts Used Here

| Concept | How it appears in this project |
|--------|-----|
| **Ops** | Individual execution steps defined with `@op` |
| **Jobs** | Directed execution graphs defined with `@job` |
| **Schedules** | Cron-based automation defined with `@schedule` |
| **Run logs** | Step-level execution and custom logs in Dagster UI |
| **Testing** | Unit-style tests for ops and schedules via `pytest` |

## Core Dagster Concepts (Expanded)

### 1) Ops: the smallest unit of work
An op is one logical step (e.g., extract CSV, validate data, load to DB). In this project, each op has a focused responsibility and logs useful context so you can debug quickly.

### 2) Jobs: execution graphs of ops
A job wires ops together into a DAG (directed acyclic graph). Dependencies between ops define execution order and data flow.

### 3) Runs: every execution is a record
Each run has a run ID, event timeline, step statuses, and logs. This gives reproducibility and observability: you can always answer "what ran, when, and why did it fail?"

### 4) Schedules: automate recurring workloads
Schedules trigger jobs on cron expressions (daily, hourly, etc.). They are where orchestration becomes operational rather than manual.

### 5) IO Managers and persisted outputs
Dagster stores step outputs between ops. In logs, you’ll see where outputs are materialized and reloaded, which helps validate data handoff correctness.

### 6) Failure semantics and reliability
Ops should fail loudly when required dependencies break (API failures, schema mismatches, quality gates). This project includes examples of explicit failure behavior and robust logging.

### 7) Data engineering mindset in Dagster
Dagster is most useful when you model pipelines around: idempotency, retry boundaries, data quality checks, and artifact persistence.

## What We’re Trying to Achieve With These DAGs

This project intentionally progresses from fundamentals to production-shaped patterns:

1. **Understand orchestration primitives** (`dag_01`, `dag_02`, `dag_03`)
  - step dependencies
  - run/event inspection
  - API integration and failure handling

2. **Implement realistic warehouse ingestion** (`dag_04`)
  - extract/stage/merge pattern
  - deterministic row IDs and deduplication
  - chunked loading for better memory behavior

3. **Model cloud orchestration structure** (`dag_05`)
  - credentials, resource setup, load/query flow
  - clear extension points for real GCP operations

4. **Treat RAG as a data pipeline** (`dag_06`)
  - ingestion → chunking → indexing → retrieval → answer generation → evaluation
  - artifact persistence for auditability and comparisons across runs

### What “good” looks like in this module
- jobs end in `RUN_SUCCESS` with clear INFO logs
- failures are understandable and actionable
- each pipeline leaves traceable outputs (tables or artifacts)
- schedules can be enabled confidently once manual runs are stable

---

## Quick Start

### Recommended Path for Your Setup (Windows 10 + Docker Desktop + VS Code Git Bash)

If you are on Windows 10 using Docker Desktop and Git Bash in VS Code, **you do not need a VM** and you can run this module directly with Docker.

Use this flow:
1. Open terminal in this folder (where `docker-compose.yml` is located).
2. Run:
  ```bash
  docker compose up -d --build
  ```
3. Open Dagster UI at `http://localhost:3000`.
4. Start running jobs from the UI.

This is the primary and recommended way to run this project.

### First 30 Minutes Checklist (Windows + Docker Desktop)

Use this checklist as your first guided walkthrough after cloning/opening this folder.

1. **Start Docker Desktop** and wait until Docker is fully running.
2. **Open Git Bash terminal in this folder** (`02-workflow-orchestration`, where `docker-compose.yml` exists).
3. **Start the stack**:
  ```bash
  docker compose up -d --build
  ```
4. **Confirm containers are healthy**:
  ```bash
  docker compose ps
  ```
  You should see `dagster_webserver`, `dagster_daemon`, and Postgres services running.
5. **Open Dagster UI** at `http://localhost:3000`.
6. **Materialize code location** (if prompted) and verify jobs are visible.
7. **Run `hello_world_job`** and watch:
  - **Timeline** for step order
  - **Logs** for step messages and outputs
8. **Run `python_stats_job`** and verify a successful external API call in logs.
9. **Run `data_pipeline_job`** to see full Extract → Validate → Transform → Load behavior.
10. **(Optional) Run RAG jobs**:
   - `chat_without_rag_job`
   - `chat_with_rag_job`
   - `interactive_chat_session_job`
11. **Inspect persisted artifacts** from RAG jobs in container storage (`/app/storage/rag_artifacts/<run_id>`).
12. **Enable schedules only after manual runs pass** (start with `hello_world_daily_schedule`).

If a run fails, use the run’s **Events** and **Logs** tabs first; they usually identify the exact failing op and reason.

### How to Run Tests

Use Docker-first test execution (recommended for consistency with your running stack):

```bash
cd 02-workflow-orchestration
MSYS_NO_PATHCONV=1 docker compose exec -T dagster_webserver pytest tests -vv -s
```

Run specific test files:

```bash
MSYS_NO_PATHCONV=1 docker compose exec -T dagster_webserver pytest tests/test_ops.py -vv -s
MSYS_NO_PATHCONV=1 docker compose exec -T dagster_webserver pytest tests/test_schedules.py -vv -s
```

Optional local execution (if you want to run tests outside Docker):

```bash
cd dagster_project
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
pytest tests -vv -s
```

Current baseline: all tests in `dagster_project/tests` pass in container.

### Prerequisites

- Docker Desktop (includes Docker Compose)
- VS Code terminal (Git Bash is fine)
- Python 3.8+ (only needed for non-Docker local dev)
- PostgreSQL client (for database interactions)
- pip (only needed for non-Docker local dev)

### Local Installation (Optional)

This section is optional. If you're already running with Docker Desktop, you can skip this.

1. **Navigate to the project directory:**
   ```bash
   cd dagster_project
   ```

2. **Create and activate a virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Start Dagster UI locally:**
   ```bash
   dagster dev
   ```

   The UI will be available at `http://localhost:3000`

---

## Project Structure

```
dagster_project/
├── dags/                              # All pipeline definitions
│   ├── __init__.py                   # Main Definitions export
│   ├── dag_01_hello_world.py         # Starter Dagster concepts
│   ├── dag_02_python_tasks.py        # External API task pattern
│   ├── dag_03_data_pipeline.py       # Core ETL pattern
│   ├── dag_04_postgres_taxi.py       # Local warehouse ingestion
│   ├── dag_05_gcp_integration.py     # GCP orchestration examples
│   └── dag_06_chat_pipeline.py       # Chat/RAG orchestration examples
├── resources/                         # Shared resources (DB, GCP, etc)
│   ├── __init__.py
│   ├── postgres.py                   # PostgreSQL connection
│   └── gcp.py                         # GCP clients
├── tests/                             # Unit and integration tests
│   ├── test_ops.py                   # Tests for individual ops
│   ├── test_schedules.py             # Tests for schedules
│   └── conftest.py                   # Pytest configuration and fixtures
├── Dockerfile                         # Docker image for containerized Dagster
├── dagster.yaml                       # Dagster configuration
├── pyproject.toml                    # Project metadata and dependencies
├── requirements.txt                  # Python dependencies
└── .gitignore                        # Git ignore rules
```

---

## Available Pipelines

### 1. Hello World (`dag_01_hello_world.py`)

Simple pipeline to learn how Dagster executes dependent steps:
- **Job**: `hello_world_job`
- **Schedule**: `hello_world_daily_schedule` (daily at 10 AM)
- **What it teaches**: Op dependencies, run lifecycle, step logs, scheduling
- **Where to see results**: Dagster UI → Runs → `hello_world_job` run → **Logs** and **Timeline**

**Run manually:**
```bash
dagster job execute -f dags/dag_01_hello_world.py -j hello_world_job
```

### 2. Python Tasks (`dag_02_python_tasks.py`)

Demonstrates Python API integration inside Dagster:
- **Job**: `python_stats_job`
- **What it teaches**: External HTTP call inside an op, structured return values, success/error logging
- **Where to see results**: Dagster UI → Runs → `python_stats_job` run → **Logs** (download count and timestamp)

**Run manually:**
```bash
dagster job execute -f dags/dag_02_python_tasks.py -j python_stats_job
```

### 3. Data Pipeline (`dag_03_data_pipeline.py`)

Complete ETL pipeline example:
- **Job**: `data_pipeline_job`
- **Steps**: Extract → Validate → Transform → Load
- **Features**: Multi-step workflows, data validation
- **Original**: `03_getting_started_data_pipeline.yaml`

### 4. PostgreSQL Taxi (`dag_04_postgres_taxi.py`)

Full ETL for NYC taxi data ingestion:
- **Jobs**: 
  - `postgres_taxi_ingest_job` (parameterized)
  - `postgres_taxi_daily_schedule` (daily execution)
- **Parameters**: 
  - `taxi` (yellow/green)
  - `year` (e.g., 2019, 2020)
  - `month` (01-12)
- **Features**: 
  - Download from GitHub
  - Conditional table schemas (yellow vs green)
  - UPSERT logic with unique IDs
  - Data deduplication

**Run example:**
```bash
dagster job execute -f dags/dag_04_postgres_taxi.py -j postgres_taxi_ingest_job \
  --config '{"taxi": "yellow", "year": "2019", "month": "01"}'
```

### 5. GCP Integration (`dag_05_gcp_integration.py`)

Google Cloud Platform workflows:
- **Jobs**:
  - `gcp_setup_job` - Initialize GCP resources
  - `gcp_data_pipeline_job` - ETL with GCP services
  - `gcp_daily_schedule` - Daily scheduled execution
- **Features**:
  - GCS bucket operations
  - BigQuery data loading
  - Query execution
- **Original**: `06_gcp_kv.yaml` through `09_gcp_taxi_scheduled.yaml`

### 6. Chat & RAG (`dag_06_chat_pipeline.py`)

AI-powered workflows with Retrieval Augmented Generation:
- **Jobs**:
  - `chat_without_rag_job` - Direct LLM responses
  - `chat_with_rag_job` - Context-aware responses
  - `interactive_chat_session_job` - Multi-turn conversations
- **Features**:
  - Document loading and embedding
  - Vector database operations
  - Semantic search
- **Original**: `10_chat_without_rag.yaml` and `11_chat_with_rag.yaml`

---

## Running Locally

If you are using Docker Desktop, you can skip this section and use **Running with Docker**.

### 1. Development Mode

Start the Dagster development server with auto-reload:

```bash
cd dagster_project
dagster dev
```

Visit `http://localhost:3000` to access the UI.

### 2. Running Specific Jobs

Execute a single job:
```bash
dagster job execute -f dags/dag_01_hello_world.py -j hello_world_job
```

Execute with configuration:
```bash
dagster job execute -f dags/dag_04_postgres_taxi.py -j postgres_taxi_ingest_job \
  --config '{
    "ops": {
      "extract_taxi_data": {
        "inputs": {
          "taxi": "green",
          "year": "2020",
          "month": "06"
        }
      }
    }
  }'
```

### 3. Running Tests

Run all tests:
```bash
pytest tests/
```

Run specific test file:
```bash
pytest tests/test_ops.py -v
```

Run with coverage:
```bash
pytest tests/ --cov=dags --cov-report=html
```

---

## Running with Docker

This is the default approach for this project on Windows + Docker Desktop.

### Prerequisites

- Docker and Docker Compose installed
- Volume for data persistence (optional)

### Startup

1. **Build and start all services:**
   ```bash
   docker compose up -d
   ```

   This starts:
   - `pgdatabase` - NYC taxi data warehouse (port 5432)
   - `pgadmin` - Database management UI (port 8085)
   - `dagster_postgres` - Dagster metadata storage (port 5433)
   - `dagster_daemon` - Scheduler and run launcher
   - `dagster_webserver` - Dagster UI (port 3000)

> Tip for Git Bash on Windows: if your folder path has spaces, always wrap paths in quotes when using `cd`.

2. **Access the UIs:**
   - **Dagster UI**: http://localhost:3000
   - **pgAdmin**: http://localhost:8085 (admin@admin.com / root)

3. **View logs:**
   ```bash
   docker compose logs -f dagster_webserver
   docker compose logs -f dagster_daemon
   ```

### Shutdown

```bash
docker compose down
```

To also remove volumes (Caution: deletes data):
```bash
docker compose down -v
```

---

## Configuration

### Environment Variables

Create a `.env` file in `dagster_project/`:

```bash
# PostgreSQL (data warehouse)
POSTGRES_HOST=pgdatabase
POSTGRES_PORT=5432
POSTGRES_DB=ny_taxi
POSTGRES_USER=root
POSTGRES_PASSWORD=root

# Dagster PostgreSQL (metadata)
DAGSTER_POSTGRES_HOST=dagster_postgres
DAGSTER_POSTGRES_PORT=5432
DAGSTER_POSTGRES_DB=dagster
DAGSTER_POSTGRES_USER=dagster
DAGSTER_POSTGRES_PASSWORD=dagster123

# GCP (optional)
GCP_PROJECT_ID=your-gcp-project
GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json

# OpenAI (optional, for chat pipelines)
OPENAI_API_KEY=your-openai-key
```

### Dagster Configuration (`dagster.yaml`)

Customize scheduler, storage, and other settings:

```yaml
scheduler:
  module: dagster_daemon.scheduler
  class: DagsterDaemonScheduler

run_launcher:
  module: dagster.run_launcher
  class: InProcessRunLauncher

storage:
  sqlite_db:
    base_dir: .dagster
```

For production, consider using PostgreSQL storage instead of SQLite:

```yaml
storage:
  postgres_storage:
    postgres_db:
      hostname: dagster_postgres
      port: 5432
      database: dagster
      username: dagster
      password: dagster123
```

---

## Troubleshooting

### Windows + Git Bash Notes

- No VM is required for this project.
- Run Docker commands from the `02-workflow-orchestration` directory.
- If containers seem stale after code changes, run:
  ```bash
  docker compose up -d --build
  ```
- If you switch branches or change dependencies, rebuild images to ensure the running code matches your files.

### Issue: Port Already in Use

**Symptom**: "Address already in use" error when starting Dagster

**Solution**:
```bash
# Kill process using port 3000
lsof -ti:3000 | xargs kill -9  # macOS/Linux
netstat -ano | findstr :3000   # Windows
```

Then on Windows (PowerShell/CMD):
```bash
taskkill /PID <PID_FROM_NETSTAT> /F
```

Or use a different port:
```bash
dagster dev -p 4000
```

### Issue: PostgreSQL Connection Failed

**Symptom**: Database connection errors in ops

**Solution**:
```bash
# Check if PostgreSQL is running
docker compose ps

# Check database connectivity
psql -h localhost -U root -d ny_taxi
```

### Issue: GCP Credentials Not Found

**Symptom**: GCP ops fail with authentication errors

**Solution**:
```bash
# Set credentials path
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json

# Verify credentials
gcloud auth application-default print-access-token
```

On Windows Git Bash, `export` works for the current terminal session. For persistent system-level env vars, use Windows Environment Variables settings.

### Issue: Docker Image Build Failed

**Symptom**: "Docker build failed" when using `docker compose up`

**Solution**:
```bash
# Rebuild the image
docker compose build --no-cache

# Then start services
docker compose up -d
```

---

## Next Steps

1. **Explore the Dagster UI**: Navigate through jobs, runs, and asset lineage
2. **Modify a pipeline**: Edit one of the DAG files and see changes in the UI
3. **Create a new pipeline**: Extend with your own custom workflows
4. **Set up alerts**: Configure notifications for failed runs
5. **Deploy to cloud**: Use Dagster Cloud or deploy to Kubernetes

---

## Resources

- [Dagster Documentation](https://docs.dagster.io)
- [Dagster Examples](https://github.com/dagster-io/dagster/tree/master/examples)
- [NYC Taxi Data](https://github.com/DataTalksClub/nyc-tlc-data)
- [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)

---

## Support

For issues or questions:
1. Check the [Dagster Slack Community](https://join.slack.com/t/dagster/shared_invite/zt-1xq6lfzfp-Z_jSWGMEPJJLDlHQKoKk-Q)
2. Review [Dagster GitHub Issues](https://github.com/dagster-io/dagster)
3. Consult the course materials and video tutorials
