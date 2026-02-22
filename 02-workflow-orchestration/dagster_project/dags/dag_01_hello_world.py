"""Hello World pipeline - 01_hello_world.yaml equivalent."""

# This file is a minimal Dagster walkthrough.
# It demonstrates: op dependencies, logging, scheduling, and a simple run lifecycle.

from dagster import op, job, schedule
from datetime import datetime


@op
def hello_message(context):
    """Step 1: log a hello message."""
    # Step output is passed downstream as a normal Python value.
    context.log.info("Hello, Will!")
    return "Hello, Will!"


@op
def generate_output(context, greeting: str):
    """Step 2: build a structured output from prior step output."""
    # We log both received input and produced payload for observability in Dagster Events.
    context.log.info(f"Received greeting: {greeting}")
    context.log.info("I was generated during this workflow.")
    return {"value": "I was generated during this workflow.", "greeting": greeting}


@op
def sleep_task(context, generated_output: dict):
    """Step 3: simulate a slow task and pass previous output through."""
    import time
    # This intentionally creates a visible delay so you can inspect timing in the UI timeline.
    context.log.info("Starting sleep for 15 seconds...")
    time.sleep(15)
    context.log.info("Sleep complete!")
    return generated_output


@op
def log_output(context, output_after_sleep):
    """Step 4: log the payload produced earlier in the pipeline."""
    # Logging payloads helps validate that data passed correctly between steps.
    context.log.info(f"This is an output: {output_after_sleep}")
    return output_after_sleep


@op
def goodbye_message(context, logged_output: dict):
    """Step 5: close the run after all upstream work is complete."""
    # This final step confirms end-of-pipeline execution and visible run completion.
    context.log.info(f"Final payload keys: {list(logged_output.keys())}")
    context.log.info("Goodbye, Will!")
    return "Goodbye, Will!"


@job
def hello_world_job():
    """Simple Dagster learning job with explicit step dependencies."""
    # Explicit variable wiring makes execution order and dependencies obvious.
    hello = hello_message()
    generated = generate_output(hello)
    slept = sleep_task(generated)
    logged_output = log_output(slept)
    goodbye_message(logged_output)


@schedule(job=hello_world_job, cron_schedule="0 10 * * *")
def hello_world_daily_schedule(_context):
    """Schedule hello world job to run daily at 10 AM."""
    # Empty config means "run with job defaults".
    return {}
