This page is the single source of truth for GoodParty's governed business
metrics, the numbers we hold ourselves to (Win and Serve activation, win rate,
cumulative wins, and more). Each metric is defined once in code, reviewed by the
people who own the part of it that changed, and published here automatically. A
metric whose **rule** is approved has an agreed meaning. A metric whose **build**
is approved has an agreed way of computing it. **Pending** on either half means
that half exists but has not been signed off yet. Nothing here is hand-edited;
it regenerates from the code definitions on every change.

## What a metric definition is made of

A metric is three layers, and each has a different owner. Splitting them is what
lets a wording fix stop expiring an approval, and stops a change to which people
get counted slipping through unreviewed.

| Layer | What it says | Who owns it | Where it lives |
|---|---|---|---|
| The rule | What we mean, in words a non-engineer can rule on | The business group | `config.meta.business_rule` |
| The implementation | Which raw events satisfy the rule | Product analytics | `config.meta.anchored_on` |
| The build | Source table, measure, aggregation, type, filter | The data group | the metric's own fields |

The rule is a structured field, not a paragraph, because prose cannot be
reviewed or sealed mechanically:

```yaml
config:
  meta:
    business_rule:
      counts: >
        A user counts once the product has performed at least one outreach
        send on their behalf.
      excludes:
        - Outreach the candidate did off-platform and self-reported.
        - Preparation to reach voters: building a list, downloading a call sheet.
      known_gaps:
        - Phone banking has no in-product send, so that channel is not counted.
```

**The test for whether a line belongs in the rule: it names no event, no surface
and no table.** The business group can say "outreach that leaves the product
counts". They cannot be asked which of three moments sharing one event name is
the send, and they should not have to be. A rule that names an event is rejected
at parse time.

The prose `description` is none of the three layers. It documents them, for a
reader. It is deliberately in neither seal.

## The two seals

Each recorded sign-off carries a fingerprint of what it approved, so the two
halves can go stale independently:

- `rule_sha` covers `business_rule` alone.
- `build_sha` covers `anchored_on`, `filter`, `measure`, `type` and `source`.

If either later changes without a new sign-off, that half's fingerprint stops
matching and this page renders it as stale, rather than letting an approval of a
superseded definition keep reading as current.

The data half also records `value_at_signing`: what the metric counted the day
it was signed. It is required. A date with no number cannot be checked by anyone
later. It proves someone looked; it never proves the number was right. Read it
as a snapshot carrying the date of the entry it sits in — these populations
drift on their own.

**What the seals do not cover**, said plainly so nobody reads them as stronger
than they are:

- **Dimensions.** They are declared once per semantic model, so sealing them
  naively would expire every metric in a file whenever one dimension was added
  anywhere. Sealing them properly means resolving which ones a given metric
  actually references, and that is not built. This covers anchor qualifiers
  that exist to compute a dimension rather than the metric's own number, such
  as `paywalled`: sealing one would expire a metric whose count never moved,
  which is the false staleness the two seals were split to remove.
- **Anything upstream of the sem file.** A change to how a mart column is
  computed moves no seal at all. This is the gap that let an OKR be signed off
  against an instrument that had been broken for five days, and go on reading
  approved for five weeks while under-counting by more than half.

## How the semantic layer is updated

Governed metric definitions are authored in one place: the dbt semantic YAML
(`dbt/project/models/**/sem_*.yml`). Sign-offs are recorded separately, in
`analytics/diagnostics/semantic_catalog/config/ratifications.yml`. Nothing on
this page is edited by hand; it is regenerated from both files on every merge.

To change a definition:

1. Edit the metric in its `sem_*.yml`. The governance block (`owner`,
   `detail_doc`, `retired` if deprecating) lives in `config.meta` alongside the
   rule and the anchor. Sign-off dates do not go here.
2. **If you changed the build, state the number.** Put the metric's value after
   your change in the PR body, as
   `<!-- semantic-value: <metric_name> = <count> -->`. It renders as nothing and
   is what gets recorded as `value_at_signing`. CI cannot compute it — that
   needs a live warehouse query — so a build sign-off with no declared value is
   not recorded at all, and the merge says so.
3. Open a pull request. CI classifies the diff by **which keys moved** and
   requests only the group that owns them:

   | What moved | Who is asked |
   |---|---|
   | `business_rule`, or `retired`, or the metric was added or removed | the business group |
   | `anchored_on`, `filter`, `measure`, `type`, `source` | the data group |
   | `description`, `label`, `owner`, `detail_doc` | nobody |

   A change can be in both lanes at once. `retired` ends the thing the business
   group ruled on, so it is their call; `era: historical` on one leg retires one
   event, which is instrumentation and sits in the data lane.
4. The group whose lane it is reviews and approves. The business group confirms
   the rule is what we mean. The data group confirms the build, the conventions,
   and value-for-value parity with the prior definition.
5. On merge, the sign-off each group's approval earned is recorded automatically
   in `ratifications.yml`, half by half, on a follow-up PR that re-requests
   nobody. A half is earned only where its own seal actually moved in that
   merge, so a PR editing one metric never ratifies the bystanders sharing its
   file.
6. Also on merge, CI regenerates the catalog and posts a change summary to the
   notification channel. **The business group is told about build changes there,
   not asked about them in review.**

Why sign-offs live in a separate file: review routing covers the `sem_*.yml`, so
writing the date there re-requests the very reviewers whose approval it records,
and forces you to write it before the approval exists. The sidecar is outside
that scope, so the recorded date is the real one.

The review gate is a soft gate. An absent approver never blocks an urgent fix.
Accountability comes from the change being visible: a merge missing an approval
the change needed is announced as exactly that, never silently.
