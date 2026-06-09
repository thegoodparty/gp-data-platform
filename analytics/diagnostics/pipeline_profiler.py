"""Profile per-stage tokens and wall-clock time for a Win-analytics pipeline run.

Reads a Claude Code session transcript (JSONL) and attributes tokens and time
to the pipeline stages, splitting wall-clock into model-active vs human-idle.
Stage boundaries come from artifact markers. Sub-agent internal tokens are not
in the transcript and are reported as a blind spot.
See analytics/planning/2026-06-09-pipeline-profiler-design.md.

Stage definitions (these four map 1:1 to the pipeline stages in the
win-analytics-process skill's references/pipeline.md, which is the authority):

  framing      Pipeline stage 1 (Frame). From the win-analytics skill load to
               the `_brief.yaml` write. Includes the scoping conversation and the
               framing-approval gate: the human-idle here is largely you
               answering scoping questions and approving the brief.
  execution    Pipeline stage 2 (Execute) plus the stage-G results checkpoint.
               From the brief write to the first reviewer dispatch. Includes
               building the analysis artifact AND the results-checkpoint gate,
               which has no artifact marker, so the idle spent waiting for
               results approval is attributed here, not to a separate stage.
  review       Pipeline stages 3 and + (the product-data-scientist and
               product-manager reviewers). From the first reviewer dispatch to
               the last reviewer result. Reviewers run in parallel, so this is a
               max-span. Tokens here are only the parent's dispatch+ingest cost;
               reviewer internal tokens are the blind spot.
  calibration  Pipeline closing step. From the first `CALIBRATION_*.md` write to
               the first human message after it (trailing off-topic work
               excluded).
"""

from __future__ import annotations

STAGES = ["framing", "execution", "review", "calibration"]
SKILL_NAMES = {"win-analytics-process", "win-analytics-knowledge"}
REVIEWER_TYPES = {"product-data-scientist", "product-manager"}
