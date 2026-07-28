## How governance works

The flow, from a definition change to a published metric:

| Stage | What happens |
|---|---|
| **1. Author** | Edit the metric in its dbt semantic YAML (`sem_*.yml`), including its `config.meta` block, and open a pull request. |
| **2. Review** (auto-requested) | CODEOWNERS requests both groups on the PR. The **data group** confirms it builds and matches the prior definition value-for-value. The **business group** confirms the definition is correct; that approval is the ratification and sets the `ratified` date. |
| **3. Publish** (on merge) | The catalog updates and a change summary posts to #data-alignment. |

Soft gate: both groups are auto-requested on every change. A merge without both approvals still goes through, but it is announced in #data-alignment as exactly that. Accountability by visibility, not by blocking.

## What this means for you

- **Using a metric?** Build on the **ratified** ones. A business owner has
  signed off that the definition is correct. **Pending** means encoded but not
  yet signed off, so use with care.
- **Need a new metric, or a change to one?** Open a pull request editing the
  metric's `sem_*.yml`. Both review groups are auto-requested, so you do not
  have to chase anyone down. A business owner's approval is what ratifies it.
- **You're a reviewer?** Data group: confirm it builds and matches the prior
  definition value-for-value. Business group: confirm the definition is what
  the business actually means. Your approval is the ratification.
- **Questions?** Post in #data-alignment.
