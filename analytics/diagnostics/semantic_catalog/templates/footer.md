## How governance works

```
   AUTHOR             REVIEW  (auto-requested by CODEOWNERS)    RECORD        PUBLISH
   ------             -----------------------------------      ------        -------

  edit the metric  open  +--------------------------------+   write the      catalog
  in its dbt YAML -> PR->| data group:                    |-> date +   -->   updates
  (sem_*.yml +           | builds + value-for-value parity|   fingerprint    +
  config.meta)           +--------------------------------+   into           change summary
                         | business group:                |   ratifications  posts to
                         | confirms the definition        |   .yml           #data-alignment
                         | = RATIFIES                     |
                         +--------------------------------+

  The date is recorded AFTER the approval it records, in a file CODEOWNERS does
  not cover, so bookkeeping never re-requests the reviewers who already signed
  off. Each date carries a fingerprint of the definition approved; if the
  definition later moves, the date renders as stale instead of looking valid.

  Soft gate: both groups are auto-requested on every change. A merge without
  both approvals still goes through, but it is announced in #data-alignment as
  exactly that. Accountability by visibility, not by blocking.
```

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
