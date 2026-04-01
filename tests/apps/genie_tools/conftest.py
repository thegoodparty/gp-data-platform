import sys
from pathlib import Path

APP_SRC = Path(__file__).resolve().parents[3] / "apps" / "genie-tools" / "src"

if str(APP_SRC) not in sys.path:
    sys.path.insert(0, str(APP_SRC))
