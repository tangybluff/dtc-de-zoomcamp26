FROM python:3.10-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy project code
COPY . .

# Set environment for Dagster
ENV DAGSTER_HOME=/app/.dagster
ENV PYTHONUNBUFFERED=1

# Ensure Dagster home exists
RUN mkdir -p /app/.dagster

# Expose ports
EXPOSE 3000

# Default command
CMD ["dagster-webserver", "-h", "0.0.0.0"]
