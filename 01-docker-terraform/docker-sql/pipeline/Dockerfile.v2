FROM python:3.13.11-slim

# This line copies the 'uv' binary from the latest official 'ghcr.io/astral-sh/uv' image.
# It uses Docker's multi-stage build to extract only the '/uv' executable from that image
# and places it into the '/bin/' directory of the current image, making 'uv' available in the container.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

 # Set the working directory inside the container to /code
WORKDIR /code
# Add virtual environment to PATH so we can use installed packages; removes the need to add "uv" "run" before entrypoint commands
ENV PATH="/code/.venv/bin:$PATH"

# Copies the dependency and environment files (pyproject.toml, .python-version, uv.lock) from the host into the container's current directory
COPY pyproject.toml .python-version uv.lock ./

RUN uv sync --locked

# Copy the pipeline.py script from the host into the container's working directory
COPY pipeline.py .

# Set the default command to run the pipeline.py script using Python
ENTRYPOINT ["python", "pipeline.py"]