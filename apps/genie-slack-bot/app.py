"""
Main application entry point for Databricks Genie Slack Bot
"""

import logging
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

from config import Config
from databricks_genie_client import DatabricksGenieClient
from slack_bot import SlackGenieBot


def setup_logging():
    """Configure application logging"""
    logging.basicConfig(
        level=getattr(logging, Config.LOG_LEVEL),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


class HealthcheckHandler(BaseHTTPRequestHandler):
    """Simple HTTP handler for Databricks App readiness checks."""

    def do_GET(self):
        """Respond to health check GET requests."""
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"ok")

    def do_HEAD(self):
        """Respond to health check HEAD requests."""
        self.send_response(200)
        self.end_headers()

    def log_message(self, format, *args):
        """Suppress default HTTP server request logging."""
        return


def start_healthcheck_server(port: int):
    """Start a background HTTP server for platform health checks."""
    server = ThreadingHTTPServer(("0.0.0.0", port), HealthcheckHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def main():
    """Main application entry point"""
    logger = logging.getLogger(__name__)

    try:
        # Setup logging
        setup_logging()
        logger.info("Starting Databricks Genie Slack Bot...")

        # Validate configuration
        Config.validate()
        logger.info("Configuration validated successfully")

        start_healthcheck_server(Config.PORT)
        logger.info(f"Healthcheck server listening on port {Config.PORT}")

        genie_client = DatabricksGenieClient(space_id=Config.DATABRICKS_GENIE_SPACE_ID)
        logger.info("Genie client initialized successfully")
        logger.info(f"Using Genie space: {Config.DATABRICKS_GENIE_SPACE_ID}")

        # Initialize and start Slack bot
        slack_bot = SlackGenieBot(
            slack_bot_token=Config.SLACK_BOT_TOKEN,
            slack_signing_secret=Config.SLACK_SIGNING_SECRET,
            slack_app_token=Config.SLACK_APP_TOKEN,
            genie_client=genie_client,
        )

        logger.info("Slack bot initialized, starting socket mode handler...")
        slack_bot.start()

    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
