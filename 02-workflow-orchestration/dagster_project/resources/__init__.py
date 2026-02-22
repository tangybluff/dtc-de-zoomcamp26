"""Shared resources for Dagster workflows."""

from .postgres import postgres_resource
from .gcp import gcp_resource

__all__ = ["postgres_resource", "gcp_resource"]
