# Answer key schema (grader-side only)

One YAML per question in this directory, named `<question_id>_key.yaml`.
NEVER copied into run arms; prep_arms deletes quality_bench from every run worktree.

Fields (see bank.py for the authoritative loader):

- `id`: question id, matches manifest.
- `as_of`: date the numbers were pinned (re-baseline convention, design §5).
- `numbers`: list of `{name, value, tolerance_pct}` — headline numbers a correct
  answer must report, graded within tolerance_pct percent.
- `required_resolutions`: map of scoping fork -> the resolution a correct answer
  must use (e.g. `denominator: cumulative_registered_upcoming_election`). Used
  three ways: cross-rep agreement in cell_consistency (gated via consistency),
  a normalized per-run comparison against the answer's ledger reported as the
  `resolutions_match` column (reported, not gated), and the judge's
  scoping_correctness score. The deterministic guard against a consistently
  WRONG resolution is the numbers: author tolerances tight enough that the
  wrong fork's number fails (key-review criterion, DATA-2144).
- `mandatory_sources`: list of `{id, pattern, description}`; `pattern` is a regex
  searched against the raw run transcript (adherence check, design §7 layer 1).
- `severity1_patterns`: regex tripwires for known confidently-wrong claims
  (cheap layer; the judge also grades confident-wrongness semantically).
  Check ids are index-based (`severity1_0`, `severity1_1`, ...), so reordering
  the patterns shifts the ids reported for a given tripwire.
- `required_assumptions`: fork names that must appear in the answer's
  assumptions ledger with a non-empty resolution (a bare fork entry does not
  count). Checked deterministically per run and reported as the
  `assumptions_pass` column in scores.csv; like `sources_pass`, it is
  reported but not gated into the pre-registered verdict rules.
- `intent_card`: 3-4 lines of asker intent (purpose, audience, fork answers).
  Unused in pass 1; pre-authored ground truth for the framing bench (design §12).
  Jot it during the gold-run key review while context is fresh.
