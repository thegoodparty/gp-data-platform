## How governance works

```
   AUTHOR                  REVIEW  (auto-requested by CODEOWNERS)        PUBLISH
   ------                  -----------------------------------          -------

  edit the metric    open    +--------------------------------+  merge   catalog
  in its dbt YAML  ->  PR -> | data group:                    | ------>  updates
  (sem_*.yml +               | builds + value-for-value parity|          +
  config.meta)               +--------------------------------+          change summary
                             | business group:                |          posts to
                             | confirms the definition        |          #data-alignment
                             | = RATIFIES (sets ratified date)|
                             +--------------------------------+

  Soft gate: both groups are auto-requested on every change. A merge without
  both approvals still goes through, but it is announced in #data-alignment as
  exactly that. Accountability by visibility, not by blocking.
```

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
