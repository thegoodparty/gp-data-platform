# Person-level ER phase 2: session handoff (rewritten 2026-09-10, third pass)

UNTRACKED working doc, do not commit. Persistent memory: `project_person_er_phase2.md`
in the Claude memory directory points here.

Worktree `.claude/worktrees/data-2244`. Switching branches here is fine and is done
routinely. Epic: **DATA-1639, Unified Entity Resolution Framework** (task `86ag662qm`).
New subtasks go under DATA-1639, not the older DATA-2075.

## Mission

A person-specific entity resolution job: a Splink `person` entity in `matcha/` plus dbt
integration, over HubSpot contacts, gp_api users, BallotReady people, and TechSpeed
records. Voter records and DDHQ are excluded; DDHQ has no person id or contact fields and
attaches via candidacy clusters.

**`gp_person_id` is authoritative.** Two records sharing one are the same person. Lower
recall is the accepted price: a false negative costs a duplicate profile, a false positive
merges two people's HubSpot contacts. This reverses the "expose, do not block" decision
recorded in the first pass of this doc.

---

# DO THIS NEXT

1. **#982 is pushed and awaiting the delegate.** Branch
   `data-2406/person-graph-probabilistic-merge`, commit `4bf88576`, rebuilt as a single
   commit on top of `origin/main`. `delegate review` was posted at 16:08. Check the
   verdict and the `dbt Cloud` check.
2. **Merge #980** (candidacy_hubspot). Approved, green, and unaffected: it suppresses on
   person-group membership and explicitly leaves merge soundness to the person graph.
3. **DATA-2396 still needs a human decision** before the merge is visible on the public
   site, but the blast radius shrank. Re-measure the retired-URL counts; the earlier
   ~37,700 and ~1,185 were against the flat union's much larger merge set.

---

## The architecture, and how it got here

Three passes. Keeping the reasoning because the two rejected shapes are both plausible and
someone will propose them again.

**Pass 1, the flat union (rejected).** Splink edges unioned into one graph, closed with
min-label propagation. 683,135 people, and 6,219 groups claiming two BallotReady people
were one human, 2,121 of them inside the HubSpot merge queue. The proposed mitigation was
to expose `br_person_count` and ask destructive consumers to skip those groups.

**Pass 2, constrain the closure (superseded by pass 3, same outcome).** Contract the
deterministic components, require complete support plus a BR cannot-link for the Splink
tier. Right answer, but it arrived as a pile of special cases: an `is_conflict` flag hand
written for E5, an abstention post-pass that detached BR records and relabelled
remainders with `br_contested` / `pass_final_key`, a separate merges model, and a
cannot-link check. Three mechanisms for one problem.

**Pass 3, shipped.** The principle the earlier passes were missing:

> Closure is sound for evidence that is transitive. It is not sound for evidence that
> merely resembles.

Sorting edges that way, instead of by which system produced them, collapses the three
mechanisms into one rule each side.

- **`int__civics_person_links`** (was `int__civics_person_edges`): what the closure runs
  over. Native identifiers (E1 HubSpot↔gp_api, E3 HubSpot→BR candidacy, E4
  ts_officeholder→BR, E6 gp_api→BR bridge, E7 within-source vendor keys) plus E5
  candidacy-stage cluster co-membership. Native identifiers are transitive by definition.
  E5 is a Splink clustering, so it is transitive only in practice, and where it is not it
  is detectable: a cluster spanning two BR people cannot say which one it means. Flagged
  `is_conflict`, same treatment reused vendor keys already had. 622 of 123,246 E5 pairs,
  and that one rule accounts for all 235 groups the deterministic tier was already fusing.
- **`int__civics_person_identities`** (was `int__civics_person_groups_deterministic`): the
  closure, and still the pregroup source. It reads no similarity edges, so the ratchet
  against a published match feeding the next run's blocking is structural now rather than
  a naming convention. A component reaching two BR people along individually sound links
  dissolves to singletons: 13 components, 78 records.
- **`int__civics_person_similarities`** (was `int__civics_person_edges_probabilistic`):
  Splink edges lifted to the identity grain.
- **`int__civics_person_groups`**: admits a set of identities only when every pair appears
  in the similarities and the result holds at most one BR person. Absorbed the pass-2
  merges model. **No propagation** — a completely supported group is exactly a set of
  identities sharing one closed neighbourhood whose size equals the set's own, so one
  aggregation finds them all. Proof is in the model header.

Eight models to six. More to the point, one principle to review instead of a list of
patches, and no edge type is a special case.

### Rejected with numbers, do not re-propose without new evidence

**Holding E5 to the completeness rule too.** Conceptually tidier, since E5 is a
similarity. Measured and rejected: **merge queue 27,598 → 17,476, a 37% loss**, people
835,522, and 205,055 identities refused for incomplete support against 30,897. The reason
is worth internalising. Closing over E5 first makes a `{BR, TS}` candidacy pair one node
carrying one Splink edge. Putting E5 and Splink in one undifferentiated graph instead
demands a Splink score on the TS↔HubSpot cross pair the matcher was never asked to
compare. **The ordering matters more than the taxonomy: close over the strong evidence,
then require completeness of the weak.**

**Raising the threshold instead of changing the closure.** 0.95 → 0.99 moves BR fusion
only 6,378 → 5,193 and gives back 3% of the merge queue. Structural rule wins outright.

**Record-grain completeness instead of identity-grain.** Drives BR fusion to zero on its
own, and provably so (a clique holding BR1 and BR2 needs a BR-to-BR edge, and the matcher
emits none; measured 0 of 6,378 multi-BR Splink clusters are cliques). But it keeps only
22,263 of 32,440 merge-queue groups against 27,598. Contraction is why the design is
affordable, and it is also why the explicit cannot-link is still needed: after
contraction, an edge from a HubSpot record inside BR1's identity to BR2 is a supported
pair of two.

**Capping a merge at two identities** instead of testing completeness: refuses 32,473
merges against 10,316. Worse on both axes.

### Held in reserve, with evidence

- **Density instead of strict completeness.** 8,634 non-clique Splink clusters sit at or
  above 0.75 density. The BR guarantee is carried independently by the cannot-link, so
  relaxing is safe on that axis. Do it only if 6.3% refusal proves too expensive.
- **Peeling.** Drop low-degree stragglers from a refused proposal and admit the dense
  core. Wait for evidence that the refused population is mostly one straggler away.
- **Move the name gate into matcha's `PERSON_POST_PREDICTION_FILTER`.** Right home for
  it, next to the BR cannot-link. Deliberately not done: the published vintage does not
  carry the gate, so removing it from dbt regresses precision until a new run is published
  and hand-swapped. Under completeness the gate matters *more*, because a bad pair between
  two singletons is now a supported pair of two and gets admitted.

### Measured, 2026-09-09 vintage

| | Prod before | Flat union | Shipped |
|---|---|---|---|
| People | 908,401 | 683,135 | 710,001 |
| Identities (pre-similarity) | 908,401 | 908,371 | 909,089 |
| Groups fusing 2+ BR people | 235 | 6,219 | **0** |
| Merge-queue groups | n/a | 32,440, 2,121 unusable | 27,598, all usable |
| Refused | n/a | none | 30,897 incomplete, 5,934 cannot-link |

**Identifier stability is exact.** 670,600 records sit in groups that neither split nor
merged, and **none changed id**. 235 prod groups split, covering 1,143 records, and they
are *exactly* the 235 that were fusing two BR people — `split_but_not_multi_br = 0`. 593
brand-new ids, all from those splits. 154,398 groups formed by merges.

Warn levels after the change: `person_links_conflict_count_warn` 1,824 (1,202 E7 + 622
E5), `person_identities_contested_rare` 78, `candidacy_stage_person_min_collision_warn`
186, TechSpeed natural-key relationship 105.

### Also changed

`candidacy_stage` prefers the person its BR key reaches over a lexical `least()` across
disagreeing native keys. Same principle, and it is what makes every stage of one
`br_candidate_id` resolve to one person (that test failed at 17 rows without it).

`people.identity_count` replaces `deterministic_subgroup_count`; `br_person_count` and
`has_probabilistic_merge` are gone, since the invariant is enforced upstream and asserted
at zero. `person_identifiers.identity_key` keeps a merge traceable.

### Risks accepted

- A human with two `br_person_id` values gets two `gp_person_id` values. E5 evidence says
  at least 235 such cases exist. That is the chosen false negative.
- Completeness is less stable run to run than closure: one edge slipping under threshold
  can flip a proposal from admitted to refused and split a group. Records that leave get
  new ids; the sub-group holding the minting member keeps its id. On the cutover this does
  not bite, because relative to prod the change is merges-only apart from the 235. Track
  the flip rate between vintages rather than designing around it.

## Where things stand

**Merged.** #908 deterministic pregroups + prematch (09-01). #910 the matcha `person`
entity (09-01). #947 `[DATA-2403]` surname-token blocking (09-09). #976 `[DATA-2405]` the
gated Splink person edge model (09-10, `e1eb2813`).

**Open.** #982 `[DATA-2406]`, the change above, awaiting the delegate. #980
`[DATA-2404]` candidacy_hubspot person suppression, approved and green.

**Published data.** `er_source.clustered_people_20260909` (996,402 rows) and
`pairwise_people_20260909` (649,625 rows), plus the live `clustered_people` /
`pairwise_people` names from that vintage. The first-creation exception is spent: **every
publish from here is dated, audited, then swapped by hand.**

**In flight elsewhere.** #900 `[DATA-1734]` the matcha ER DAG is still open and its
`ENTITIES` list has no person lane. Adding one is a single `EntitySpec` plus gate
thresholds, after it merges.

## Binding architecture decisions

**Close over the strong evidence, then require completeness of the weak.** The ordering,
not the taxonomy. See the rejected E5 experiment above for what happens if you flatten it.

**matcha does not resolve identity. It emits suggestions; dbt decides.** Injecting
pregroups upstream (built, reviewed, removed) did the same union twice.

**dbt consumes the PAIRWISE table, not the clustered one.** `clustered_people` is Splink's
own connected components at 0.95, so consuming it imports the exact defect this work
removed and gives up the ability to veto an edge. Under completeness that veto is the
control surface rather than something chaining routes around. `clustered_people` stays
published for audit with no dbt consumer.

Still in force:

- Matcha keeps its single predict, post-filter, cluster flow. Full cohort every run.
- **NEVER write a live `er_source` table.** Dated snapshot, audit, manual swap.
- Merges of two distinct gp_api users are ALLOWED. `people.gp_api_user_ids` is the array;
  the scalar `gp_api_user_id` still nulls on ambiguity (2,609 people).
- **No Jr/Sr cannot-link.** Of five hand-verified suffix-conflict pairs, three were the
  same person. `suffix_token` is audit only.
- **E8/E9 deterministic contact edges stay DROPPED** (PR #907, built then closed). Raw
  contact-key equality over-merges; the same information now arrives through Splink with
  name agreement attached, which is what makes it safe.
- **The `gamma_last_name > 0` filter stays declined.** It would delete ~11,000 correct
  merges to prevent at most a few hundred questionable ones.
- **Only add code with empirical evidence** that it prevents a known false positive or
  negative. This killed four post-prediction filter clauses and the suffix cannot-link.

## Phase D evaluation

Artifact **Person Matcher Phase D**:
https://claude.ai/code/artifact/d4c94307-14cb-4202-9668-f693a60e44d3

Still valid, with one reading changed: the precision-census failure and the BR fusion
finding were symptoms of the closure, not of the pair scores.

| Gate | Result | Bar |
|---|---|---|
| E1/E3 recall, email-exact | 99.51%, 99.71% | 98% |
| E1/E3 recall, email-differs | 86.51%, 98.68% | 70% soft |
| Exact contact + exact name | 99.63% of 296,747 pairs | 99% |
| EM sanity, fitted email m | m = 0.5481, u = 6.4824e-7 | not degenerate |
| Identifier stability | 0 churn / 0 new ids | zero |
| Precision census | failed | <= 2% |

**Precision census, 396,474 cross-pregroup merging edges.** Shared contact key 197,227
safe; exact name same state+city 85,044 safe; exact name same state one city unknown
47,579 safe; exact name same state cities differ 50,356 (mostly the BR district-in-city
artifact); surname exact first name abbreviates 5,068 safe; **surname exact first name
genuinely differs 4,168, 6 wrong in 50 sampled**; first name exact surname differs 6,821
safe; both differ 211 excluded with the bad class. The bad class signature: no shared
contact key, first names agreeing only because the alias arrays intersect. Every error
crossed gender or joined two distinct given names (antonio/antoinette, dennis/denise,
nancy/hannah, joan/jon, john/ian, kayla/catherine). The gate costs 3,923 of 595,329
merging pairs (0.66%). Abbreviations (ben/benjamin, rick/richard) were correct in every
pair read and stay.

**Holdout, pregroup blocking off.** Matched clusters barely move (224,493 → 224,263) but
**E1 email-differs blocking coverage falls 95.48% → 66.94%**. `block_on("pregroup_id")`
stays.

## Measurements worth keeping

**Current vintage (`matcha/results/person_20260909/`).** 996,402 prematch records; 649,625
scored pairs; 617,332 clusters; 224,493 matched (223,444 cross-source, 1,049
within-source); largest cluster 54. Match rates: ballotready 33.95%, gp_api 94.67%,
hubspot 80.48%, techspeed 88.53%, techspeed_officeholder 98.03%. Calibration: 83.5% of
merging edges at p >= 0.9999, 3.3% in the 0.95-0.99 band.

**Graph shape.** 909,089 identities: 84% singletons, 97.5% three records or fewer. Of
non-BR records inside a BR identity, 89% are directly adjacent to the BR record. The
closure is running over stars of size two and three, which is why it costs 25 seconds.

**Completeness rate.** 74% of similarity proposals are a single edge between two
identities, where completeness is trivially satisfied. It only bites on the remaining 26%,
which is where chaining lives. Clique rate by proposal size at the identity grain: 2
components 100%, 3 85%, 4 68%, 5-9 72%, 10+ 18%.

**Candidacy clusters.** 457,218 clusters, max size 9, 91,388 multi-member, 161,988 pairs
in the complete-graph expansion. Safe to cross join if ever needed.

**The three largest Splink clusters (54, 33, 24) are internal test fixtures**, not
real-person errors. Placeholder surnames, dummy text in city. All three are non-cliques,
so this design refuses them. Worth a hygiene ticket.

**Identifier stability is structural.** `gp_person_id` is `md5(minting source_name +
source_id + 'person')` where the minting member is earliest by `first_seen_at`. The
earliest member of a union is necessarily the minting member of one pre-merge group, so a
pure merge can only land on an id that already exists. Ties break by `(source_name,
source_id)` with `source_id` compared as a **string**, so two HubSpot contacts from one
bulk import are decided by lexical id order, not age.

## Traps worth knowing

- **The dbt Cloud CLI kills its invocation when backgrounded.** Twice this session a
  `nohup dbt build` reported exit 0 with a log stopping at "Running dbt...", built one
  model, then died server-side. **Never diagnose progress from table timestamps** — that
  cost 20 minutes of polling a corpse. `dbt list` is the probe: if it returns instead of
  saying "Session occupied", nothing is running. Run builds in the FOREGROUND; the whole
  person chain is about two minutes.
- **The dev session is single-occupancy and SHARED WITH SUBAGENTS.** Give subagents the
  SQL statements API instead.
- **`dbt build` on a Python model fails in dev**: the token lacks the `jobs` scope
  (`int__civics_viability_scoring`). Its downstream tests then fail on stale relations and
  cannot be cleared locally. CI builds it.
- **dbt dev schemas beat prod deferral.** A model you did not modify resolves to production
  even when something you DID modify sits upstream, so an unselected intermediate silently
  invalidates a whole verification. Select the full chain by name, and rebuild dependents
  one and two tiers out before trusting a relationship test.
- **`git reset --soft origin/main` on a stale branch stages the REVERSION of every commit
  that landed meanwhile.** It looked like a clean 12-file diff and was actually deleting
  four other people's PRs. Save the intended files, `reset --hard`, copy back.
- **`er_source` tables are ALL-STRING columns.** `coalesce(match_probability, 0)` errors
  with CAST_INVALID_INPUT; `>= 0.95` works via implicit cast. Cast explicitly.
- **Null-safe comparisons in edge classification.** `a.email = b.email` is NULL when one
  side is null, so a `when contact then ...` branch silently misroutes ~3,600 rows.
- **`people` and `person_identifiers` live in `mart_civics`**, not
  `goodparty_data_catalog.dbt`.
- **Databricks lateral column alias shadowing.** An alias IS readable later in the same
  select list, but a FROM-clause column of the same name silently wins. This bit #947.
- **Regex escapes do not survive the yaml-to-Jinja-to-SQL round trip.** `\p{L}` arrives at
  Spark as `p{L}` and errors. Keep test predicates backslash-free.
- **A yaml `>` folded scalar keeps a newline** if a continuation line is indented deeper
  than the first. Harmless in SQL, but keep continuations at one level.
- **`min_label_propagation` keys on a column literally named `record_key`.** A caller
  whose nodes are something else has to alias into that name.
- **A red `dbt Cloud` check can just be the CI run cancelled by your own merge.** Read
  `status_message`. The token is `token-value` (not `token`) on line 12 of
  `~/.dbt/dbt_cloud.yml`.
- **Worktree push refspec.** `push.default = upstream` means a bare `git push origin
  <branch>` resolves the DESTINATION to `main`. ALWAYS `git push origin branch:branch`.
- **Shell cwd persists between tool calls**, and a `cd x && ...` that fails silently runs
  the rest of the line in the wrong place. Use absolute paths.
- `dbt build --select "a b c"` needs the quotes.
- Prod ad-hoc SQL: `databricks api post /api/2.0/sql/statements`, warehouse
  `18583d8b081c6486`. INLINE truncates around 12K rows; chunk or use EXTERNAL_LINKS.
- `system.query.history` covers SQL warehouses only. dbt Cloud model builds are not in it.
- A full matcha person run takes about 6 minutes locally with
  `MATCHA_DUCKDB_MEMORY_LIMIT=12GB`.
- **A push DISMISSES the delegate's standing approval.** Later rounds need a standalone
  `delegate review` comment.
- **Production DDL is blocked from a Claude session** by the permission classifier. Hand
  the `create table ... as select` to the owner.
- PR titles `[DATA-XXXX] ...`, branches `data-xxxx/slug`, ticket ids never in committed
  code, no Claude attribution, no internal people's names in any written artifact. Public
  candidate names are fine.

## Reproducible recipes

**Completeness rate of a proposed merge set.** Per identity, `sort_array(collect_set(
neighbour))` over the undirected similarity edges plus a self-loop; group by that array;
admitted when `count(*) = size(array)`. No propagation. The same shape works against the
raw `er_source` tables using `clustered_people.cluster_id` as the blob and `unique_id` as
the node, which gives the record-grain lower bound.

**Identifier stability, done right.** Do NOT compare `person_group_key` between runs: the
key survives when a group merely gains members, so "same key, different id" counts merges
as churn (it read 60,621 before I fixed it). Instead map records prod→dev, count
`distinct dev_group` per prod group and `distinct prod_group` per dev group, and only
groups with 1 on both sides are untouched. Churn among those must be zero.

**Fitted m for a Splink comparison level.** Solve the two Bayes factors simultaneously
(`m1/u1` and `m0/u0`, with `m0+m1=1`, `u0+u1=1`). Do NOT back m out of an empirically
computed u; that gave 1.125 and looked broken. The chart HTML has no inline data.

**Pregroup-nulled holdout, no Databricks write.** Rewrite the run's `input.parquet` with
`pregroup_id = unique_id`, re-serialize array columns with `json.dumps` (parquet returns
ndarrays, and `to_csv` would write Python reprs), write CSV, pass as `--input`.

**TechSpeed key expansion.** The prematch keys techspeed on the person-grain candidate
code; the person graph keys it per candidacy stage. Map with `strip_ts_stage_suffix` on
`int__civics_person_nodes.record_key`. Every other source is 1:1. Verified: 94,066 of
94,077 stripped node person-keys match a prematch key.

## Still open

- **DATA-2396, retired profile URLs.** The only item needing a decision from outside the
  data team. Re-measure under the shipped design; the earlier figures were against the
  flat union. No redirect or slug-history mechanism exists. DATA-2397 (slug tie-break) is
  DONE.
- **Person lane in the matcha ER DAG.** Blocked on #900.
- **Runbook for the person lane** in `matcha/README.md`.
- **Review the refused proposals.** `rejected_reason` makes the 30,897 addressable. A
  sample read would say whether `incomplete_support` is mostly blocking gaps (pairs never
  compared) or genuinely loose proposals, which decides whether density or peeling is
  worth building.
- **Pregroups shrank** by however much E5-only components contributed, since
  `int__civics_person_identities` is now the pregroup source and still includes E5 — so
  actually unchanged. Confirm on the next matcha run that `pregroup_id` counts match.
- **DATA-2395** alias + institutional-phone false positives. Largely covered by the gate;
  re-measure before closing.
- **DATA-2398** pharma-spam HubSpot contacts in the prematch, 764 merging edges.
- **Unfiled: test-fixture hygiene in the prematch.** The three largest clusters. Same class
  as the existing `last_name = 'user'` and `%party registrar%` filters.
- **Unfiled: normalize the BR city field.** It holds a DISTRICT name, which is what makes
  the 50,356-edge cities-differ class look risky when it mostly is not.
- **Unfiled: `first_name_tokens` has the same ASCII-shattering bug** #947 fixed for
  surnames; 6,520 records have an empty array. Lower impact: that column feeds a
  comparison level with a JaroWinkler fallback rather than a blocking rule.
- **Unfiled: surnames sharing no token still cannot block together.** Needs a surname
  comparison level tolerant of token containment, not another blocking rule.

## Scratch and dev assets

- `dbt_dball` holds the shipped models plus the civics marts, rebuilt 09-10 16:0x.
  `int__civics_viability_scoring` is stale and unbuildable locally.
- `matcha/results/person_20260909/` is the current vintage. `person_holdout_20260909/` is
  the pregroup-nulled run. `tokens/` is the pre-suffix-fix run; `nodinject` and anything
  older is injection-era, ignore.
- `dbt_dball.scratch__e8e9_universe`, the old study universe. Safe to drop.
- Two known matcha performance items, neither in scope: `_normalize_to_strings` in
  `cli.py` makes roughly 17.8M per-cell Python calls, and the filtered-pairs sidecar.
