"""Per-batch isolation spot-check (README no-decision gate 2 interim,
ClickUp 86ajmyk2q). Run isolation is a permission boundary, not a filesystem
one: a run executes as the operator's user and could in principle read the
original checkout (answer keys) or sibling runs. Until container-grade
isolation exists, every decision-bearing batch gets its transcripts scanned
for out-of-arm filesystem access, and the operator accepts the result in
writing on the ticket.

A run's allowed filesystem surface: its own run dir (the transcript cwd), its
ephemeral HOME (<run_id>.home beside the run dir), tmp dirs, and system
prefixes. Any other absolute path in a Read/Write/Edit/Glob/Grep input or
embedded in a Bash command is flagged for human review — flags are review
items, not verdicts.
"""

from __future__ import annotations

import argparse
import json
import posixpath
import re
from pathlib import Path

try:
    from quality_bench import bank  # noqa: F401  (package-anchored import check)
except ImportError:  # bare `python spot_check.py`: diagnostics/ isn't on sys.path yet
    import sys

    sys.path.insert(0, str(Path(__file__).parents[1]))

PATH_TOOLS = ("Read", "Write", "Edit", "NotebookEdit", "Glob", "Grep")
# Prefixes any run may touch: tmp and OS/toolchain locations. The operator's
# home is deliberately absent — the run's own dirs are allowed explicitly.
SYSTEM_PREFIXES = (
    "/tmp/",
    "/private/tmp/",
    "/private/var/",
    "/var/",
    "/dev/",
    "/usr/",
    "/bin/",
    "/sbin/",
    "/opt/",
    "/etc/",
    "/System/",
    "/Library/",
    "/Applications/",
)
ABS_PATH = re.compile(r"(?<![\w@.-])/(?:[\w.@-]+/)*[\w.@-]+")
# Relative tokens that climb via `..` — invisible to ABS_PATH but resolved
# against the run cwd they can escape it (Bugbot, PR #686).
REL_DOTDOT = re.compile(r"(?<![\w@./-])(?:[\w.@-]+/)*\.\.(?:/[\w.@-]+)*")


def _allowed(path: str, cwd: str, run_prefixes: tuple[str, ...]) -> bool:
    """Lexical containment check: relative paths resolve against the run cwd and
    `..` segments are normalized before prefix matching, so a path that *starts*
    under an allowed prefix but climbs out via `..` is not allowed. Lexical only
    (paths are gone by check time); symlink escapes are out of scope for this
    interim — flags are review items, not verdicts."""
    resolved = posixpath.normpath(path if path.startswith("/") else posixpath.join(cwd, path))
    probe = resolved + "/"
    return any(probe.startswith(p) for p in (*run_prefixes, *SYSTEM_PREFIXES))


def flag_transcript(transcript_file: Path) -> list[str]:
    """Return one line per suspicious filesystem access in a run transcript."""
    cwd = ""
    accesses: list[tuple[str, str]] = []  # (tool, path-or-command)
    for raw in transcript_file.read_text().splitlines():
        try:
            entry = json.loads(raw)
        except json.JSONDecodeError:
            continue
        cwd = cwd or entry.get("cwd", "")
        content = (entry.get("message") or {}).get("content")
        if not isinstance(content, list):
            continue
        for c in content:
            if not (isinstance(c, dict) and c.get("type") == "tool_use"):
                continue
            tool, tool_input = c.get("name", ""), c.get("input") or {}
            if tool in PATH_TOOLS and (p := tool_input.get("file_path") or tool_input.get("path")):
                accesses.append((tool, p))
            elif tool == "Bash" and (cmd := tool_input.get("command")):
                accesses.extend(("Bash", m) for m in ABS_PATH.findall(cmd))
                accesses.extend(("Bash", m) for m in REL_DOTDOT.findall(cmd))
    if not cwd:
        return ["no cwd recorded in transcript; cannot establish the allowed surface"]
    run_dir = cwd.rstrip("/")
    run_prefixes = (run_dir + "/", f"{run_dir}.home/")
    return [f"{tool}: {p}" for tool, p in accesses if not _allowed(p, run_dir, run_prefixes)]


def check_batch(batch_dir: Path) -> dict[str, list[str]]:
    """Flag every ok run in a batch; a missing transcript is itself a flag
    (an unreviewable run cannot be accepted)."""
    state = json.loads((batch_dir / "state.json").read_text())
    out: dict[str, list[str]] = {}
    for run_id, run_state in state["runs"].items():
        if not run_state.get("ok"):
            continue
        t = run_state.get("transcript_file")
        if not t or not Path(t).exists():
            out[run_id] = ["transcript missing"]
            continue
        out[run_id] = flag_transcript(Path(t))
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch", required=True)
    args = parser.parse_args()
    batch_dir = Path(__file__).parent / "results" / args.batch
    results = check_batch(batch_dir)
    flagged = {rid: flags for rid, flags in results.items() if flags}
    print(f"spot-check: {len(results)} runs scanned, {len(flagged)} with flags")
    for rid, flags in sorted(flagged.items()):
        for f in flags:
            print(f"  {rid}: {f}")
    raise SystemExit(1 if flagged else 0)


if __name__ == "__main__":
    main()
