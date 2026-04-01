"""Test package shim for local genie_tools imports."""

from pathlib import Path
from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)

APP_PACKAGE = (
    Path(__file__).resolve().parents[3] / "apps" / "genie-tools" / "src" / "genie_tools"
)
if str(APP_PACKAGE) not in __path__:
    __path__.append(str(APP_PACKAGE))
