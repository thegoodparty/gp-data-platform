# Person id retirement handoff

How a published person id stops existing and how the data platform hands election-api a
forwarding address for it. How an id is minted is in `dbt/project/CLAUDE.md` ("Post-ER
identifier derivation"); the short version is that `gp_person_id` is the hash of a cluster's
earliest member, recomputed every run, so it retires whenever that member stops being earliest,
most often because two clusters merged. The public URL is `/people/<first-last>-<id8>` and
resolves on `id8`, the first 8 hex of the id, so a retired id's page would 404 without a forward.

## The contract

`PersonMerge` in election-api, one row per retired id, written only by the `sync_election_api`
DAG in the same swap transaction as `Person`:

| column | what we publish |
|---|---|
| `retired_id` | the retired `gp_person_id`; no FK, its Person row is gone |
| `surviving_id` | the terminal survivor; no FK, it may itself retire later |
| `retired_slug` | the slug as last published; null when we never captured it |
| `retired_at` | the later of leaving Person and leaving the mint; one batch shares a timestamp |
| `created_at` | build timestamp, like Person |

1. **`surviving_id` is the final survivor.** It is the id the retired id's minting record carries
   today, and that record already sits in the final cluster, so `A -> B -> C` publishes as
   `A -> C` and `B -> C` with no chain walking.
2. **The row lands in the same run as the delete**, because the swap set is one transaction.
3. **Many-to-one is normal.** The reverse cannot occur: an id has exactly one minting record.

## Where the rows come from

Two dbt snapshots, neither of which can be rebuilt:

- `snapshot__int__civics_person_canonical_ids` records every id the mint has produced and the
  record that minted it. An id in that history that nobody carries today is retired, and its
  minting record's current id is the survivor.
- `snapshot__m_election_api__person` records each published id's slug and closes the row when
  the id leaves the mart, which gives `retired_slug` and `retired_at`. An id minted before this
  snapshot began is kept when it retires whether or not the snapshot saw it; ids minted after it
  get a row only if they were published.

## For consumers

This table is a routing map, not an event log. A row's `surviving_id` is rewritten in place when
its survivor is absorbed, a row disappears when its id becomes live again, and a row held back for
an unpublished survivor appears later with an older `retired_at`. A consumer following only the
`(retired_at, retired_id)` cursor misses all three: reconcile the full table periodically and use
the cursor between reconciles.

When `retired_slug` is null, the API reconstructs it from the survivor's name, so a same-name
duplicate still redirects; a name-variant duplicate that also shares its 8-hex prefix with a live
person is a 404 rather than a guess.

## Accepted gaps

- A minting record that vanished upstream cannot be mapped; its id stays a 404, even if a
  forwarding row was published earlier, because the table is recomputed each run.
- A retired cluster whose members scattered follows its minting record.
- A survivor that is not yet a public profile holds its rows back until it is.
- Grandfathered rows include alias ids that were never published; they are inert.
- Legacy full-uuid URLs, the gp-api poller, and the gp-marketing redirect status are app-side.

## Operating notes

- **Neither snapshot may be dropped, recreated, or made `appendOnly`** (closing a row is an
  update). `dbt build --full-refresh` leaves snapshots alone by design.
- **Recovery.** Both snapshots keep 90 days of Delta history via a post_hook (predictive
  optimization vacuums this schema on the default 7-day window). Undo a bad run with
  `RESTORE TABLE <snapshot> TO TIMESTAMP AS OF '<before the run>'` (needs MODIFY); recover a drop
  with `UNDROP TABLE <snapshot>` within the schema's recovery window (owner or MANAGE).
- **The Person sync refuses a swap when under 90% of live ids survive.** A dedup pass that
  retires more than a tenth of Person in one run fails the whole swap set closed; batch it or
  lower `min_id_overlap` for that run, deliberately.
