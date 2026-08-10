This page is the single source of truth for GoodParty's governed business
metrics, the numbers we hold ourselves to (Win and Serve activation, win rate,
cumulative wins, and more). Each metric is defined once in code, reviewed by
both a data owner and a business owner, and published here automatically. A
metric marked **ratified** has an agreed definition that is safe to build
dashboards, reports, and AI answers on. **Pending** means the definition exists
but has not been signed off yet. Nothing here is hand-edited; it regenerates
from the code definitions on every change.

## How the semantic layer is updated

Governed metric definitions are authored in one place: the dbt semantic YAML
(`dbt/project/models/**/sem_*.yml`). Sign-off dates are recorded separately, in
`analytics/diagnostics/semantic_catalog/config/ratifications.yml`. Nothing on
this page is edited by hand; it is regenerated from both files on every merge.

To change a definition:

1. Edit the metric in its `sem_*.yml`, including its `config.meta` governance
   block (`owner`, `detail_doc`, and `retired` if deprecating). The ratified
   date does not go here.
2. Open a pull request. CODEOWNERS automatically requests a reviewer from the
   data group and the business group.
3. A data-group reviewer confirms the build, conventions, and value-for-value
   parity with the prior definition.
4. A business-group reviewer confirms the definition is correct. That approval
   is the ratification.
5. Once both approvals are in, record the sign-off in `ratifications.yml`: the
   date the second group approved, the definition fingerprint, and the PR
   number. The fingerprint comes from `semantic_catalog.cli --fingerprints`.
   Do this on the same PR before merging, or in a follow-up PR.
6. On merge, CI regenerates the catalog and posts a change summary to the
   notification channel, including whether both groups approved.

Why the date lives in a separate file: CODEOWNERS covers the `sem_*.yml`, so
writing the date there re-requests the very reviewers whose approval it records,
and forces you to write it before the approval exists. The sidecar is outside
that glob, so the recorded date is the real one.

Each recorded sign-off carries a fingerprint of the definition it approved. If
the definition later changes without a new sign-off, the fingerprint stops
matching and this page renders the date as stale, rather than letting a date
that certifies a superseded definition keep reading as approved.

The review gate is a soft gate. An absent approver never blocks an urgent fix.
Accountability comes from the change being visible: a merge without both
approvals is announced as exactly that, never silently.
