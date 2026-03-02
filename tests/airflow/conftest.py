"""Shared fixtures and path setup for Airflow tests."""

import os
import sys

# Add astro dir to path so tests can import include.custom_functions
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "..", "airflow", "astro")
)
