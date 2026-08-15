"""Generic SFTP helpers: connect, list, download. Nothing here knows about a particular feed."""

import logging
import re
import time

from paramiko import SFTPClient, Transport
from paramiko.sftp_attr import SFTPAttributes

logger = logging.getLogger("airflow.task")

# paramiko reads 32 KiB per request, so an unprefetched download is round-trip bound.
_PREFETCH_REQUESTS = 64


def create_sftp_connection(
    host: str,
    port: int,
    username: str,
    password: str,
    max_retries: int = 3,
    retry_delay: int = 5,
) -> tuple[Transport, SFTPClient]:
    """Open an SFTP connection with keep-alive, retrying transient failures."""
    for attempt in range(max_retries):
        transport = None
        try:
            transport = Transport((host, port))
            transport.set_keepalive(30)
            transport.connect(username=username, password=password)
            sftp_client = SFTPClient.from_transport(transport)
            if sftp_client is None:
                raise ValueError("Failed to create SFTP client")
            logger.info("SFTP client created successfully")
            return transport, sftp_client
        except Exception as e:
            if transport is not None:
                transport.close()
            logger.error(f"SFTP connection attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                raise
            logger.warning(f"Waiting {retry_delay} seconds before next attempt...")
            time.sleep(retry_delay)
    raise Exception("Failed to establish SFTP connection after all retries")


def list_matching(
    sftp_client: SFTPClient, remote_dir: str, pattern: re.Pattern
) -> list[tuple[SFTPAttributes, re.Match[str]]]:
    """The files in remote_dir whose names match pattern, each with its match."""
    matches = ((a, pattern.match(a.filename)) for a in sftp_client.listdir_attr(remote_dir))
    return [(attributes, match) for attributes, match in matches if match is not None]


def download(sftp_client: SFTPClient, remote_path: str, local_path: str) -> None:
    sftp_client.get(
        remotepath=remote_path,
        localpath=local_path,
        max_concurrent_prefetch_requests=_PREFETCH_REQUESTS,
    )
