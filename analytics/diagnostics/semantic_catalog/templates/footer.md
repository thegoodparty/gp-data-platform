## How governance works

The flow, from a definition change to a published metric:

| Stage | What happens |
|---|---|
| **1. Author** | Edit the metric in its dbt semantic YAML (`sem_*.yml`), including its `config.meta` block, and open a pull request. If you changed how the number is computed, state its new value in the PR body. |
| **2. Route** (automatic) | CI reads which keys moved and requests only the group that owns them. Changed what the number MEANS? The business group. Changed how it is COMPUTED, or which events feed it? The data group. Changed only the wording, the label or the owner? Nobody. |
| **3. Review** | The **business group** confirms the rule is what we mean. The **data group** confirms it builds, and matches the prior definition value-for-value. Each approval signs its own half. |
| **4. Record** | Each half's date, a fingerprint of what it approved, and — for the build half — what the metric counted that day are written to `ratifications.yml`. The dates are recorded AFTER the approvals they record, in a file routing does not cover, so bookkeeping never re-requests the reviewers who already signed off. If either half later moves, that half renders as stale instead of looking valid. |
| **5. Publish** (on merge) | The catalog updates and a change summary posts to #data-alignment. The business group is **told** about build changes here, not asked about them in review. |

Soft gate: a merge missing an approval the change needed still goes through, but
it is announced in #data-alignment as exactly that. Accountability by visibility,
not by blocking.

## What this means for you

- **Using a metric?** Look at both halves. **Rule approved** means a business
  owner has signed off that the meaning is right. **Build approved** means the
  data group has signed off on how it is computed, and recorded what it counted
  that day. **Pending** on a half means that half has not been signed off yet.
  **Stale** means the content changed after it was approved, so treat that half
  as pending until it is re-signed.
- **Need a new metric, or a change to one?** Open a pull request editing the
  metric's `sem_*.yml`. The right group is requested for you, so you do not have
  to work out who to chase. If you changed how the number is computed, put its
  new value in the PR body — nothing else can compute it for you.
- **You're a business reviewer?** You are ruling on `business_rule`: what counts,
  what is excluded, and which gaps we accept. It names no event and no table on
  purpose. You should never be asked which of three events is the right one; if
  a review seems to be asking that, it was routed wrong.
- **You're a data reviewer?** Confirm the build, the conventions, and
  value-for-value parity with the prior definition, and check the value stated
  in the PR body against what you would expect.
- **Questions?** Post in #data-alignment.
