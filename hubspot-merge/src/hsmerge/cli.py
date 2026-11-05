"""hsm, the merge job's command line. The commands and their arguments are a later step."""

from __future__ import annotations

import sys

NOT_WIRED_MESSAGE = "hsm: this command is not wired up yet"


def main() -> int:
    print(NOT_WIRED_MESSAGE, file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
