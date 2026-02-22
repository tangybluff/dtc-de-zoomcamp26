"""Python tasks pipeline - 02_python.yaml equivalent."""

# This file demonstrates a classic orchestration pattern:
# call an external API, validate response, log key metrics, and fail clearly on errors.

import requests
from dagster import op, job, Failure


@op
def collect_stats(context):
    """
    Query GitHub API to get repository statistics for Dagster.
    
    This op demonstrates:
    - Making external API calls
    - Processing and returning structured data
    - Logging context information
    """
    context.log.info("Fetching GitHub statistics for dagster-io/dagster...")
    
    def get_repo_metadata(repo_name: str = "dagster-io/dagster"):
        """Query GitHub API and return key metadata for a public repository."""
        # Keep this helper local to the op so the example remains self-contained.
        url = f"https://api.github.com/repos/{repo_name}"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    
    try:
        # Parse only fields we care about for a simple operational health snapshot.
        metadata = get_repo_metadata()
        stars = metadata.get('stargazers_count', 'Not available')
        forks = metadata.get('forks_count', 'Not available')
        last_updated = metadata.get('last_updated')
        context.log.info(f"Dagster repo stars: {stars}")
        context.log.info(f"Dagster repo forks: {forks}")
        context.log.info(f"Dagster repo updated at: {last_updated}")
        
        return {
            'status': 'success',
            'repository': 'dagster-io/dagster',
            'stars': stars,
            'forks': forks,
            'last_updated': last_updated,
        }
    except Exception as e:
        # Raising Failure marks the run as failed (instead of silently returning a failed payload).
        context.log.error(f"Error fetching Dagster repo stats: {str(e)}")
        raise Failure(f"Failed to fetch Dagster repo stats: {str(e)}")


@job
def python_stats_job():
    """Job that collects and processes Python statistics."""
    # Single-op job on purpose: easy to inspect and validate in Dagster UI.
    collect_stats()
