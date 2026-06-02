import sys
from pathlib import Path

# analytics/lib is not an installed package; put it on sys.path so tests can
# `import win_analysis` (same convention as analytics/lib/README.md usage).
LIB_SRC = Path(__file__).resolve().parents[2] / "analytics" / "lib"

if str(LIB_SRC) not in sys.path:
    sys.path.insert(0, str(LIB_SRC))
