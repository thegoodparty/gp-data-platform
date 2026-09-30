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
reviewed or sealed mechanically. It is authored under `config.meta.business_rule`
with three keys: `counts` (one sentence on who is included), `excludes` (a list
of what deliberately does not count), and `known_gaps` (a list of what we accept
we cannot measure yet).

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

## When the instrument stops firing

A seal compares a file against a record. Both are documents, and neither can see
that the events a metric declares have stopped arriving. So the catalog also
reads the instrument-health monitor's latched dormant anchors, and a metric whose
declared event has gone quiet renders its build approval as **needs
re-verification**, with the reason shown under the catalog: the metric name, the
rule half unchanged, the build half marked NEEDS RE-VERIFICATION, and a reason
line naming the declared event and the date it last fired.

It clears the way the monitor's own latch clears: the event recovers, or
`anchored_on` adopts the successor. **There is deliberately no dismissal.** This
is evidence, not an opinion, and silencing it should mean fixing the instrument
or changing what the metric is anchored on.

Two things to know about where this runs. It is a cross-repo network read, so it
applies on the catalog page and in the merge summary, **not** in the blocking
catalog-freshness gate, which stays offline and deterministic. And when the read
fails, the page says the check did not run rather than rendering everything as
healthy — a guard that disables itself quietly is the failure this exists to
remove, rebuilt one layer up.

When the read fails, the merge summary does **not** carry that. The notification
channel is for metric news, and a cross-repo token the review groups cannot
provision is not something they can act on; it goes to the metric owner as a
direct message instead, falling back to the channel only if the DM is refused.
What does reach the channel is the metric-level warning itself: a metric merging
while its declared instrument is latched dormant says so there, because that is
metric news.

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

### Changing the machinery, not a metric

Work on the layer's plumbing — the seals, the sidecar schema, review routing,
the catalog generator — changes no metric's meaning, so the notification channel
should not hear about it.

- Open the pull request as a **draft** while it touches a governed `sem_*.yml`.
  A draft posts no thread anchor and asks no review group. A pull request that
  touches no `sem_*.yml` cannot post at all, so open that one ready for review —
  the review bot will not look at a draft, and leaving it in draft only strands
  it.
- On merge, a summary is skipped only when no metric's record moved at all.
  That is narrower than it sounds: re-stamping or reformatting a definition
  still moves the record, and so does recording a sign-off.
- So for a migration that re-stamps definitions without changing any meaning,
  label the pull request `governance:mechanics` and its summary goes to the
  owner as a direct message instead of the channel. The label does most of the
  work here; the automatic skip is the cheap half.

The label redirects the summary; it never deletes it. Nothing can quietly
suppress the announcement of a real metric change.

Why sign-offs live in a separate file: review routing covers the `sem_*.yml`, so
writing the date there re-requests the very reviewers whose approval it records,
and forces you to write it before the approval exists. The sidecar is outside
that scope, so the recorded date is the real one.

The review gate is a soft gate. An absent approver never blocks an urgent fix.
Accountability comes from the change being visible: a merge missing an approval
the change needed is announced as exactly that, never silently.

## When the product and a metric drift apart

A metric names raw events, and the product keeps changing which events fire, so
the two drift apart. There are three ways it happens, and each has a different
owner. Name which one you are in before changing anything, because that decides
who approves and in what order.

| Drift | What happened | Who decides | What changes | Review lane |
|---|---|---|---|---|
| **A. The meaning changed** | The business group rules that the metric should count something different | The business group, first | `business_rule`, then `anchored_on` re-derived to match it | Business and data |
| **B. The product moved, the meaning did not** | An event was renamed, a surface was rebuilt, or an event stopped firing and a successor took over | Product analytics | `anchored_on`, plus any model that names the event directly | Data |
| **C. The product changed what could be counted** | A new surface or property would widen or narrow what a leg counts, in a way the rule does not mention | The business group, before any pull request | Nothing until they rule. Then it proceeds as A (change the rule) or B (change the implementation so the rule still holds) | Set by the ruling |

These are the cases omni's alignment monitor raises (`anchor_alignment.py`). Its
case 2, the declaration is behind the product, is drift B. Its case 3, the two
disagree on scope, is drift C. Its case 1, omni's registry is behind the
declaration, is the last step of A and B rather than a drift of its own.

**One change can hold two drifts.** A rename (B) can carry a property change that
quietly lets a new group in (C). Settle the C half first. The B half proceeds
alongside only where it does not depend on the ruling.

**For drift B, keep the old leg.** History before the change lives only under the
old event, and a leg cannot be limited to a date range, so replacing the leg
deletes that history from the metric.

### Steps, for any drift

1. **Name the drift.** For A or C, get the business group's ruling in writing
   first. Nobody decides what a metric means by choosing which events to count.
2. **Read the gotchas books before planning:** omni's
   `packages/runbooks/books/analytics-governance-gotchas.md`, and this repo's
   `.claude/skills/win-analytics-knowledge/references/gotchas.md` (or the Serve
   one). Every row is a trap that has already produced a confident wrong answer.
3. **Pin the dates from the warehouse**, not from a ticket: when the product
   change reached prod, when the old event last fired, when the new one first
   fired.
4. **Prove removal before retiring a leg.** `era: historical` needs the commit or
   pull request that removed the old event from the code. An event going quiet is
   not proof.
5. **Find every reader, more than one way:** the literal event name in both
   repos, the macros that compile `anchored_on`, and any model that
   de-duplicates or branches on the event's properties. One search that finds
   nothing is not evidence.
6. **Compare the old and new events' properties, in code and in data.** Every
   `excluding` qualifier must still exclude exactly what it did, and every key a
   model de-duplicates on must still exist. A property change that admits or
   drops a group the rule does not name is drift C: stop and take it to the
   business group.
7. **Measure the gap** since the change: who is missing from the metric today.
8. **Ship in this order:**
   1. A preparation pull request that touches no `sem_*.yml`: the models and
      tests that name the event directly, and any fix the new leg needs in order
      to count correctly. Merge it first.
   2. The `sem_*.yml` pull request: the new leg, the old leg kept with
      `era: historical` and its date, the pin tests updated, and the metric's new
      value in the body. Opening it starts the review routing above.
   3. Mart documentation (`m_*.yaml`) in its own small pull request, because an
      edit there rebuilds a large part of the project in CI.
   4. The omni pull request that brings the monitoring registry in line
      (`monitored_events.yaml` and its tests).
9. **Verify after merge:** the sign-off pull request opened, the catalog
   regenerated, the metric's prod value matches the value stated in the pull
   request, and any dormant-instrument warning cleared. Anything predicted while
   planning, such as "no step at the cutover", is measured here, not assumed.
10. **Update the docs that describe the metric** in the same ticket.

The procedure for an agent working a drift, with the queries and commands, is in
omni's `triage-instrumentation-gaps` skill (Queue C). It follows this section and
restates none of it.
