# Canonical person: implementation plan

Plan for reworking civics entity resolution around a canonical `Person` object with a
stable `gp_person_id` and crosswalks to every source system. Companion to
`canonical-entity-rework.md` and `civics_person_er_context.md`.

## Approach in one paragraph

Keep candidacy-stage (and election-stage) matching as the first probabilistic layer, since
compound facts carry the most discriminating signal. Matcha keeps its single
predict -> filter -> cluster flow over the full record cohort every run; deterministic
identity is data it consumes: dbt resolves deterministic links into a `pregroup_id` per
record, matcha scores those pairs like any others (so Splink's probability estimation sees
the whole cohort) and guarantees them at cluster time. The probabilistic `person` layer
therefore only decides the genuine residual (TechSpeed candidates, DDHQ candidates, HubSpot
contacts without BR FKs, and the candidate-vs-office-holder divide). Mint `gp_person_id`
with an earliest-member rule based on source-native created timestamps: fully re-derivable,
safe under dbt full refresh, stable while cluster membership churns. Runs recur daily behind
the existing snapshot -> loss gate -> swap promotion, extended with a lost-pairs metric and
a crosswalk snapshot. Rebuild the mart around a `people` dimension carrying every
source-native person identifier, add `gp_person_id` FKs through the civics mart, re-mint the
published ids in one shot (no consumers depend on their stability), and expand ER coverage
from 2026+ to all-time, sharded by election year.

## Design decisions

### 1. Resolution is layered: deterministic, then fact-derived, then probabilistic

Order of trust:

- L1: native-id edges (no inference). gp_api `user` <-> HubSpot `contact`
  (bidirectional ids), HubSpot contact `br_candidate_id` -> BR person, HubSpot contact
  `br_candidacy_id` -> BR candidacy -> BR person, TS officeholder `ts_officeholder_id` ==
  `br_office_holder_id` -> BR person (guarded by the existing reuse flag), gp_api campaign
  `hubspot_id` <-> HubSpot company, and BR's own `br_candidate_id` unifying every BR
  candidacy and term.
- L2: existing candidacy-stage and election-stage Splink runs.
- L3: edges derived from L2. Two source records in the same candidacy-stage cluster are the
  same person by definition (cluster grain is person x race-stage). This is how "matched
  candidacies feed person matching" is realized concretely.
- L4: `person` Splink run deciding everything L1 and L3 leave open.

The deterministic edge graph is shallow: every edge points into one of two hubs
(`br_candidate_id` or `prod_db_user_id`), and a HubSpot contact carrying both bridges them.
Two or three join passes resolve it to a `pregroup_id` per record; no general
connected-components step is needed in dbt.

### 2. Matcha keeps predict -> filter -> cluster; deterministic links ride along as data

One matcha invocation per entity, as today. Deterministic identity enters through a single
column on the prematch, not through code paths or extra CLI inputs:

- Every source record stays its own row in `int__er_prematch_persons`, carrying the
  dbt-computed `pregroup_id`. No proto-record collapse: Splink's u/m estimation works on
  the whole cohort, and the deterministic pairs (known true matches) are scored like any
  other pair. Hiding them would remove exactly the pairs that best calibrate the model.
- `block_on("pregroup_id")` guarantees every deterministic pair is generated and scored.
- A new generic `EntityConfig.deterministic_grouping_column` field: after post-prediction
  filters and before clustering, matcha appends same-group edges at probability 1.0, so a
  weak probabilistic score can never break a known link. This is the whole hard-positive
  mechanism; no `--forced-edges`, no `--exclusions`.
- Hard-negative gates remain `post_prediction_filters` in the entity configs, applied
  between predict and cluster. They are pure person-identity guards; they no longer carry
  known-id logic.
- Auditability: matcha writes the post-filter surviving pairs to Databricks
  (`er_source.filtered_pairs_<entity>`) alongside the unfiltered pairwise table, replacing
  the pod-local `filtered_pairs.csv` sidecar.
- Optional later improvement, not planned work: use the pregroup pairs as labels for
  Splink's m-estimation (`estimate_m_from_pairwise_labels`) instead of relying solely on EM.

Considered and rejected: collapsing pregroups into proto-records before matching (starves
probability estimation, see above); splitting matcha into predict and cluster commands with
a dbt edge-policy hop between (awkward three-step lane); clustering unfiltered pairs and
overriding membership downstream in dbt (connected components chain on any false-positive
edge, and un-merging a cluster requires re-running components on the filtered subgraph,
which SQL handles poorly).

### 3. Candidacy facts become explicit person-matching features

The person prematch carries, per record:

- `candidacy_stage_cluster_ids` (array): drives an `ArrayIntersectLevel` comparison and a
  blocking rule, same pattern `election_stage` already uses with
  `matched_candidacy_stage_clusters` (`matcha/scripts/configs/election_stage.py:70`).
- `offices_run_tokens` (array of office-name tokens across the record's known
  candidacies/terms) and `election_dates` (array): supporting comparisons that say "these
  two people ran for the same kinds of offices on the same dates" without over-weighting
  any single office.
- Person-stable attributes: first/last name plus `first_name_aliases` and
  `first_name_tokens` (reusing the nickname seed machinery), email, phone, state,
  birth_date (TS and HubSpot only), socials.

Office and date arrays are supporting evidence only. The person post-prediction filter
requires person-stable agreement (last name plus one of first-name signal, email, phone,
birth_date, or a shared candidacy cluster) so two different people in the same race can
never merge on office signal alone.

### 4. Stable ids: earliest member by source-native timestamp, no source preference

Rule for `gp_person_id` per final person cluster:

`gp_person_id = generate_salted_uuid([source_name, source_id], salt='person')` of the
cluster member that is earliest by first-seen ordering, ties broken by
(source_name, source_id) sort. No BR preference: if a gp_api user exists before the person
surfaces in BallotReady, the id keys off the gp_api record and does not change when the BR
record arrives. First-in wins uniformly, whatever the source.

First-seen ordering is computed in a plain, fully re-derivable model
(`int__civics_record_first_seen`), safe under dbt full refresh. No insert-only incremental
state. Per-source precedence:

1. Source-native created timestamp where the source records one: gp_api `created_at`,
   HubSpot `createdate`, BR `created_at` on the v3 rows (verify presence in the pre-work;
   TS `date_processed` as its nearest equivalent).
2. Else earliest source-file date (`_ab_source_file_url` for the gdrive feeds).
3. Else `min(_airbyte_extracted_at)`, accepting that a full Airbyte re-extract rewrites
   these and triggers a coordinated id rebuild. This fallback should end up covering few or
   no sources.

Properties: stable under cluster growth and merges (the earliest member of a union is the
min of the earliest members) and deterministic under splits (the half without the earliest
member re-derives its own). Known caveat: a late-arriving record with an older source-native
created timestamp can claim the mint for an existing cluster on the next build. Rare, and
accepted in exchange for full-refresh safety.

The same rule re-mints `gp_candidacy_id`, `gp_election_stage_id`, and the other published
ids (decision 8), so TS/DDHQ-first races and candidacies keep a stable id even when a BR
record arrives later.

### 5. Recurring runs: full-cohort rerun daily, behind the existing promotion gate

Splink runs on the entire cohort every time; no incremental matching. Protection against a
bad run wiping prior matches is a promotion pipeline, not match-layer state, and most of it
already exists in the `run_matcha_er` DAG:

- Each run writes timestamped snapshot tables (`clustered_<entity>__<ds>` etc.). This is
  the staging-table step: a run never touches the live tables directly.
- The loss gate compares the snapshot against the live table and fails the run (manual
  checkpoint, no retries) when loss exceeds threshold. Extend the metrics with an explicit
  "previously-linked pairs lost" count, so silent erosion of known matches trips the gate
  even when aggregate loss looks small.
- Only a passing run gets the atomic `CREATE OR REPLACE` swap; pruning keeps the last 8
  snapshots for rollback.
- Add a dbt snapshot on `int__civics_person_canonical_ids` so cluster-membership and id
  assignment changes are diffable over time and recoverable.

Id stability does not depend on freezing cluster membership: membership may churn run to
run, while the decision-4 mint keeps `gp_person_id` fixed for the overwhelming majority of
clusters. Cadence moves from weekly to daily once runtimes are confirmed; EM retrains each
run, which is desirable (distributions track the cohort).

### 6. BallotReady GraphQL extraction moves to Airflow and becomes person-first

The S3 _v3 files are denormalized candidacy and term rows; BR person identity has to be
reconstructed from them, which is why `int__ballotready_candidate_identity` exists. The
CivicEngine GraphQL API has a first-class Person object.

Consistent with the broader migration of ingress/egress out of dbt and into Airflow:

- New Airflow DAG (`ballotready_graphql_extract`) that incrementally extracts GraphQL
  objects to S3, each run fetching only new or changed records (cursor on person ids seen in
  the S3 feeds minus persons already extracted, plus a staleness window). Ingested to
  staging like any other S3 source.
- Start with Person (the critical-path object: bio, contacts, experiences). Coverage target:
  every `br_candidate_id` observed in either S3 feed.
- The existing dbt python extraction models (`int__ballotready_person.py`,
  `int__ballotready_candidacy.py`) are deprecated once the DAG-fed staging tables reach
  parity.
- `int__civics_person_source_ballotready` reads the API person as the authority for person
  attributes, falling back to S3-derived fields where missing. API contacts and experiences
  feed the person prematch (extra emails/phones, prior offices held).
- Optional follow-on, off the critical path: extract Race, Election, Position, OfficeHolder
  via the same DAG pattern. The S3 FKs already give the person layer what it needs.
- Practicalities: API ids are base64 node ids while S3 uses integer databaseIds
  (`_base64_encode_id` converts); the DAG needs rate-limit handling and backfill batching
  for the initial historical sweep.

### 7. Person scope: every human in any system, identifiers as columns

The `people` mart includes all gp_api users (Win, Serve, poll-only), all HubSpot contacts
(including those with no candidacy context and no user link; they resolve deterministically
or sit as singletons), all BR people, all TS and DDHQ candidates.

One column per source-native person identifier where it exists: `br_person_id` (BR's person
databaseId, today's `br_candidate_id`), `gp_api_user_id`, `hs_contact_id`,
`ddhq_candidate_id` (scalar where unambiguous; full multi-valued sets in
`person_identifiers`). Resolution status is then a query, not a flag:
`hs_contact_id is not null and br_person_id is null` finds CRM contacts unresolved to
BallotReady.

Role flags, derived from resolved relationships: `is_candidate` (at least one candidacy
record from any source) and `is_elected_official` (at least one officeholder/term record
from any source). `candidate` remains derivable as "person where is_candidate".

### 8. Published ids change in one shot

No one relies on the current published ids being stable, so there is no migration, no
side-by-side cycle, no mapping model. `gp_candidate_id` (-> `gp_person_id`),
`gp_candidacy_id`, and `gp_election_stage_id` swap to the cluster-derived decision-4 rule
directly, with downstream civics models updated in the same change and the election_api
feed repointed immediately after. The attribute-hash macros (`generate_ts_gp_*`,
`generate_gp_api_gp_*`) are deleted; attribute hashing stops being an identity mechanism
anywhere.

### 9. ER coverage expands from 2026+ to all-time

`int__er_prematch_candidacy_stages` currently filters to `election_day >= 2026-01-01`, and
2025 lives in HubSpot-archive spine models. For a person entity this window is wrong:
a person's 2024 candidacies and 2018 terms are exactly the signal that distinguishes them.

- Person projections are all-time from day one.
- Candidacy-stage and election-stage ER expand backward in election-year shards. The
  same-stage gate (election_date equality) makes year-sharding lossless for candidacy-stage
  matching: no cross-year pair can survive the filters anyway, so each year can be matched
  independently. This also keeps DuckDB memory inside the current pod sizing (4 CPU / 8Gi);
  raise resources or batch years per pod run as measured.
- Elected-official/term data is not date-gated the same way and already spans history.
- The 2025 archive spines join the person universe through their deterministic keys
  (HubSpot contact ids, gp_api user ids) and their records enter the person projections.
  Back-resolving archive candidacies into candidacy-stage clusters happens with the
  year-shard backfill.

## Target architecture

```
Airflow: ballotready_graphql_extract -> S3 -> staging (BR person objects)

staging (per source)
  -> int__civics_person_source_* (person projections: BR candidacy, BR officeholder,
     BR api person, TS candidate, TS officeholder, DDHQ candidate, gp_api user,
     HubSpot contact)
  -> int__civics_record_first_seen (source-native created-at ordering, re-derivable)
  -> int__civics_person_edges (L1 deterministic edges, typed)
  -> int__civics_person_pregroups (edge resolution -> pregroup_id per source record)

per entity (candidacy_stage, election_stage, person; elected_official until retired):
  int__er_prematch_<entity>  (person rows carry pregroup_id)
    -> matcha match (predict -> post-prediction filters -> +pregroup edges -> cluster)
    -> er_source snapshots -> loss gate -> swap to pairwise_/filtered_pairs_/clustered_
    -> stg_er_source__*

  -> int__civics_person_canonical_ids (cluster -> gp_person_id, earliest-member;
     dbt-snapshotted)
  -> marts: people, person_identifiers, and gp_person_id FKs on candidacy,
     candidacy_stage, elected_officials, elected_official_terms, users, campaigns
  -> election_api feed (rightmost consumer)
```

## Work plan: discrete PRs, leftmost to rightmost

Each item below is intended as one standalone PR that leaves the system consistent and
shippable. Items within a track are sequential; tracks A, B, and C are largely parallel
until D.

### Pre-work (analysis, no PRs)

Verification queries informing the PRs; do these first:

1. DDHQ `candidate_id` stability across races/years (decides DDHQ's projection grain).
2. TS officeholder -> BR person coverage after excluding reused ids.
3. HubSpot contact FK coverage: `br_candidate_id`, `br_candidacy_id`, `goodparty_user_id`
   counts and overlap.
4. Fragmentation baseline: `gp_candidate_id`s per `br_candidate_id`; unadopted TS/DDHQ
   person-hashes. The before-metric for the whole rework.
5. Source-native timestamp audit per source (which created-at fields exist, lag between
   creation and ingestion). Finalizes the first-seen precedence.
6. API person coverage vs the S3 feeds; sizes the extraction backfill.
7. Candidacy-stage record volume per election year; sizes the backfill shards and pods.

### Track A: ingestion and extraction (leftmost)

- A1. Airflow `ballotready_graphql_extract` DAG: incremental Person extraction to S3, with
  rate-limit handling and backfill batching. Includes the initial historical sweep as a
  backfill run.
- A2. Staging for the landed BR API person objects: `__sources.yaml` entry plus
  `stg_*` model(s).

### Track B: dbt identity foundations

- B1. `int__civics_record_first_seen`: one row per (source_name, source_id) with
  `first_seen_at` per the decision-4 precedence. Tests: uniqueness, not_null,
  `first_seen_at <= _airbyte_extracted_at` sanity.
- B2. Person source projections, part 1 (civics vendors): BR candidacy, BR officeholder,
  BR api person (depends on A2), TS candidate, TS officeholder, DDHQ candidate. All-time,
  uniform schema (native keys and FKs, person attrs, created timestamp, candidacy/term
  context keys). Uniqueness tests.
- B3. Person source projections, part 2 (product/CRM): gp_api user, HubSpot contact. Same
  schema and tests.
- B4. `int__civics_person_edges` (typed deterministic edges, conflict flags) and
  `int__civics_person_pregroups` (hub resolution to pregroup_id). Tests: one pregroup per
  record, one br_candidate_id per BR-containing pregroup, warn-severity conflict counts.

### Track C: matcha

- C1. Housekeeping: write post-filter surviving pairs to Databricks
  (`er_source.filtered_pairs_<entity>`) replacing the CSV sidecar; generalize the
  hardcoded list-column parsing into `EntityConfig.list_columns`. Register new tables in
  `__sources.yaml` with staging models.
- C2. `deterministic_grouping_column` support in the pipeline: `block_on` the column when
  configured, append same-group edges at probability 1.0 after filters, before clustering.
  Unit tests with a synthetic fixture.
- C3. `PERSON_CONFIG` (`matcha/scripts/configs/person.py`) registered in
  `entity_config.py`: comparisons (last_name JW+TF; first_name ladder; email; phone;
  birth_date; state; candidacy_stage_cluster_ids, offices_run_tokens, election_dates as
  `ArrayIntersectLevel`), blocking (pregroup_id; shared candidacy cluster; email; phone;
  state + last_name variants; birth_date + last_name), the person-stable post-prediction
  filter (decision 3), thresholds 0.01/0.95,
  `deterministic_grouping_column="pregroup_id"`. Unit tests plus a labeled fixture from
  known gp_api<->HubSpot pairs.
- C4. `int__er_prematch_persons` (depends on B4 and the live candidacy-stage clusters):
  one row per source record with pregroup_id and the decision-3 features.
- C5. er_source output contract for person: `__sources.yaml` entries and
  `stg_er_source__{pairwise,filtered_pairs,clustered}_persons` staging models.
- C6. DAG changes (in the `matcha-airflow-schedule` worktree): person lane after the
  candidacy_stage swap; pregroup models added to the prematch-refresh job selector;
  "previously-linked pairs lost" metric added to the loss gate; cadence to daily once
  runtimes are confirmed.

### Track D: canonical ids and marts

- D1. `int__civics_person_canonical_ids`: explode clusters to (source_name, source_id),
  apply the earliest-member mint, output gp_person_id + cluster/pregroup lineage. Includes
  the dbt snapshot on this model. Tests: one gp_person_id per cluster; each
  br_candidate_id and gp_api user id maps to exactly/at most one gp_person_id.
- D2. `people` and `person_identifiers` marts: identity-level precedence
  (gp_api > HubSpot > BR > TS > DDHQ for contact fields, BR > others for civic fields),
  per-source identifier columns, `is_candidate` / `is_elected_official`. Full test block in
  `m_civics.yaml`. Parity audit against the pre-work fragmentation baseline lands with this
  PR as an analysis artifact; manual merge/split sample review gates D3.
- D3. `gp_person_id` FKs added to `candidacy`, `candidacy_stage`, `elected_officials`,
  `elected_official_terms`, `users`, `campaigns`, `candidate`, with relationships tests.
- D4. Id re-mint, one shot: `gp_candidate_id` (aliased to gp_person_id or replaced),
  `gp_candidacy_id`, `gp_election_stage_id` move to the cluster-derived rule; window-max
  "adopt BR canonical" cascades in `int__civics_candidate_{techspeed,gp_api}` replaced with
  joins to `int__civics_person_canonical_ids`; attribute-hash macros deleted;
  `dbt/project/CLAUDE.md` ID table and mart README updated.
- D5. election_api feed update (rightmost): repoint the `write__election_api_db` models and
  the candidacy_hubspot reverse ETL onto the new ids.

### Track E: backfill and retirement

- E1. All-time expansion: remove the 2026 date filter from the prematch models, year-shard
  the candidacy-stage/election-stage runs (DAG parameterization), backfill historical years
  per the pre-work volume check.
- E2. Retire the `elected_official` Splink entity if parity holds: person link comes from
  person ER; term selection keeps the date-proximity bridge
  (`int__civics_elected_official_gp_api_bridge`). Prune candidacy-stage post-prediction
  filter clauses that only enforced deterministic-id agreement now handled by pregroups.
- E3. Delete `int__ballotready_candidate_identity` (API-first BR projection proven) and the
  dbt python GraphQL extraction models (Airflow DAG at parity).

## Validation strategy

- Labeled evaluation: hold out the gp_api<->HubSpot deterministic links (drop them from
  pregroups in an offline run) and measure how many the person model recovers (recall) and
  whether it proposes contradicting links (precision). The one free labeled dataset.
- Reuse matcha's audit machinery with person-appropriate `false_negative_group_cols`
  (state + last_name + birth_date). The `filtered_pairs_*` tables make gate behavior
  inspectable from SQL.
- Loss gate with the lost-pairs metric guards every recurring run (decision 5).
- Regression: re-run the DDHQ-miss-audit style check after D4 to confirm person unification
  did not disturb candidacy-stage results.

## Open questions

1. DDHQ `candidate_id` stability (pre-work query 1 decides DDHQ's projection grain).
2. Whether `gp_candidate_id` survives as an alias column for `gp_person_id` on `candidate`
   or is dropped outright in D4. Cosmetic; decide at D4 review.
