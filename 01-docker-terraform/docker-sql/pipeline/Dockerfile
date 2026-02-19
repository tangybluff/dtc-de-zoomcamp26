# Dockerfile for building a Python-based data ingestion pipeline
#
# Dockerfiles define the steps to build a container image. Each instruction creates a new layer in the image.
# The typical structure is:
#   1. Specify a base image (FROM)
#   2. Copy files or set up dependencies (COPY, RUN)
#   3. Set environment variables (ENV)
#   4. Set working directory (WORKDIR)
#   5. Define the default command (ENTRYPOINT or CMD)
#
# This format ensures reproducibility, portability, and isolation for applications.

# 1. Start from a minimal Python 3.13 image
FROM python:3.13.11-slim

# 2. Copy the 'uv' dependency manager binary from its image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# 3. Set the working directory inside the container
WORKDIR /code

# 4. Add the virtual environment's bin directory to PATH
ENV PATH="/code/.venv/bin:$PATH"

# 5. Copy dependency files and lock file for reproducible installs
COPY pyproject.toml .python-version uv.lock ./

# 6. Install dependencies using 'uv' with the lock file
RUN uv sync --locked

# 7. Copy the application code
COPY ingest_data.py .

# 8. Set the default command to run the ingestion script
ENTRYPOINT ["python", "ingest_data.py"]