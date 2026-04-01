"""
Configuration management for the Databricks Genie Slack App
"""

import os


class Config:
    """Application configuration — all values sourced from environment variables."""

    # Slack
    SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
    SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET")
    SLACK_APP_TOKEN = os.getenv("SLACK_APP_TOKEN")

    # Databricks — host + auth handled automatically by the SDK.
    # In Databricks Apps the SDK reads the auto-provisioned service principal.
    # For local dev set DATABRICKS_HOST and DATABRICKS_TOKEN in env.
    DATABRICKS_GENIE_SPACE_ID = os.getenv("DATABRICKS_GENIE_SPACE_ID")

    # App
    _port_value = os.getenv("DATABRICKS_APP_PORT") or os.getenv("PORT") or "3000"
    PORT = int(_port_value)
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    @classmethod
    def validate(cls):
        """Validate required environment variables are present."""
        required = [
            "SLACK_BOT_TOKEN",
            "SLACK_SIGNING_SECRET",
            "SLACK_APP_TOKEN",
            "DATABRICKS_GENIE_SPACE_ID",
        ]
        missing = [v for v in required if not getattr(cls, v)]
        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}"
            )
        return True
