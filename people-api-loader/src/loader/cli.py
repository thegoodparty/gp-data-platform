"""Top-level CLI entry — currently delegates to the people-api consumer.

As additional consumers land they can either be folded in here as
subcommand groups, or shipped as separate `pyproject` console scripts.
"""

from __future__ import annotations

from loader.people_api.cli import main

__all__ = ["main"]


if __name__ == "__main__":
    main()
