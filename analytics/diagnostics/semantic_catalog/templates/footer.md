## How governance works

The flow, from a definition change to a published metric:

| Stage | What happens |
|---|---|
| **1. Author** | Edit the metric in its dbt semantic YAML (`sem_*.yml`), including its `config.meta` block, and open a pull request. |
| **2. Review** (auto-requested) | CODEOWNERS requests both groups on the PR. The **data group** confirms it builds and matches the prior definition value-for-value. The **business group** confirms the definition is correct — that approval is the ratification. |
| **3. Record** | The ratification date and a fingerprint of the approved definition are written to `ratifications.yml`. The date is recorded AFTER the approval it records, in a file CODEOWNERS does not cover, so bookkeeping never re-requests the reviewers who already signed off. If the definition later moves, the date renders as stale instead of looking valid. |
| **4. Publish** (on merge) | The catalog updates and a change summary posts to #data-alignment. |

Soft gate: both groups are auto-requested on every change. A merge without both approvals still goes through, but it is announced in #data-alignment as exactly that. Accountability by visibility, not by blocking.

## What this means for you

- **Using a metric?** Build on the **ratified** ones. A business owner has
  signed off that the definition is correct. **Pending** means encoded but not
  yet signed off, so use with care. A date marked **stale** means the definition
  changed after it was approved, so treat it as pending until it is re-signed.
- **Need a new metric, or a change to one?** Open a pull request editing the
  metric's `sem_*.yml`. Both review groups are auto-requested, so you do not
  have to chase anyone down. A business owner's approval is what ratifies it,
  and the date is written down once both approvals are in.
- **You're a reviewer?** Data group: confirm it builds and matches the prior
  definition value-for-value. Business group: confirm the definition is what
  the business actually means. Your approval is the ratification.
- **Questions?** Post in #data-alignment.
