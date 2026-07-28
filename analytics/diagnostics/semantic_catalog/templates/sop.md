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
(`dbt/project/models/**/sem_*.yml`). Nothing on this page is edited by hand; it
is regenerated from that YAML on every merge.

To change a definition:

1. Edit the metric in its `sem_*.yml`, including its `config.meta` governance
   block (`owner`, `ratified`, `detail_doc`, and `retired` if deprecating).
2. Open a pull request. CODEOWNERS automatically requests a reviewer from the
   data group and the business group.
3. A data-group reviewer confirms the build, conventions, and value-for-value
   parity with the prior definition.
4. A business-group reviewer confirms the definition is correct. That approval
   is the ratification; the same change sets the `ratified` date.
5. On merge, CI regenerates the catalog and posts a change summary to the
   notification channel, including whether both groups approved.

The review gate is a soft gate. An absent approver never blocks an urgent fix.
Accountability comes from the change being visible: a merge without both
approvals is announced as exactly that, never silently.
