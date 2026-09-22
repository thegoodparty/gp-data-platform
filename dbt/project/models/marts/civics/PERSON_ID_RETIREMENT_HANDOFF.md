# Person id retirement handoff

How a published canonical person id stops existing, and how the data platform hands
election-api a forwarding address for it. This is the counterpart to the origination
rules in `dbt/project/CLAUDE.md` ("Post-ER identifier derivation"), which cover how an id is
minted. The omni migration, Prisma model, and `persons.service.ts` all point here.

## Why ids retire

`gp_person_id` (the app's `gp_candidate_id`, same value) is the salted hash of one record: the
member of the person's entity-resolution cluster that is earliest by `first_seen_at`. Every
co-member carries that id. The pipeline is rebuilt from scratch each run, so an id has no
existence outside the current build. It retires whenever its minting record stops being the
earliest member of its cluster:

- two clusters merge and the other cluster's minter is earlier (the routine dedup case)
- an older record arrives late and joins the cluster (a mint steal)
- the minting record disappears from its source entirely

The public profile URL is `/people/<first-last>-<id8>`, and it resolves on `id8`, the first
8 hex of the id, not on the name. When the id retires, `Person` is swap-replaced without it and
the URL is unaddressable. `PersonMerge` is the forwarding address.

## The contract

`PersonMerge` in election-api, one row per retired id:

| column | type | what we publish |
|---|---|---|
| `retired_id` | uuid, PK | the retired `gp_person_id`; no FK, its Person row is gone |
| `surviving_id` | uuid, not null | the terminal survivor; no FK, it may itself retire later |
| `retired_slug` | text, null | the slug as last published; null when we never captured it |
| `retired_at` | timestamp(3), not null | the later of leaving Person and leaving the mint; one batch shares a timestamp |
| `created_at` | timestamp(3), not null | build timestamp, like Person |

Rules the app relies on, and how the mart meets them:

1. **`surviving_id` is the final survivor, with fan-in rewritten.** The survivor is the id the
   retired id's minting record carries today. That record already sits in the final cluster, so
   `A -> B -> C` is published as `A -> C` and `B -> C` in the same run, with no chain walking.
   The API still walks up to four hops defensively.
2. **The row lands in the same run as the delete.** `PersonMerge` is a member of the
   `sync_election_api` swap set, which renames every staged table into place in one transaction.
3. **Many-to-one is the normal shape.** Several minting records in one cluster give several rows
   with one `surviving_id`. The reverse (one retired id, several survivors) cannot occur: an id
   has exactly one minting record.

## Where the rows come from

- `snapshot__int__civics_person_canonical_ids` (check strategy on the id and its minting record,
  hard deletes invalidated) has recorded every id the mint has produced since the Person table
  went live. An id in that history that is no longer anyone's `gp_person_id` is retired, and the
  history names the record that minted it.
- `snapshot__m_election_api__person` (id, slug; check strategy on slug, hard deletes invalidated)
  supplies the slug as last published and the run that closed the id's last row. It also says
  whether the id was ever published at all: alias ids that were never live get no row.
- `m_election_api__person_merge` joins the two, keeps only survivors that are live public
  profiles, and stamps `created_at`.

An id minted before the slug snapshot began is kept when it retires whether or not the snapshot
ever saw it, since it may have been published and left the mart before capture started; if the
snapshot never saw it, `retired_slug` is null. The API reconstructs a missing slug from the
survivor's name, so a same-name duplicate still redirects; a name-variant duplicate that also
shares its 8-hex prefix with a live person is a 404 rather than a guess. Those grandfathered
rows include alias ids that were never published, which are inert. Ids minted after the snapshot
began get a row only if they were published.

## Write boundary and pull direction

Only the `sync_election_api` DAG writes `PersonMerge`, exactly as with `Person`; the application
never writes it. gp-api drains `GET /v1/person-merges` with a keyset cursor on
`(retired_at, retired_id)` to repoint its own person ids. `retired_at` is stable for a given
retirement and identical across a batch. It is the later of the id leaving Person and leaving the
mint: an id can drop out of the public mart while still canonical and retire later, and stamping
the earlier exit would land the row behind the cursor.

This table is a current routing map, not an event log. A row's `surviving_id` is rewritten in
place when its survivor is itself absorbed (`A -> B` becomes `A -> C` with no new row), a row
disappears when its id becomes live again, and a row held back for an unpublished survivor
appears later with an older `retired_at`. A consumer that only follows the cursor misses all
three. Reconcile the full table periodically (it is small) and treat the cursor as a way to pick
up new retirements between reconciles.

## Accepted gaps

- A minting record that vanished upstream cannot be mapped; its id stays a hard 404. That holds
  even for a row published earlier: the table is recomputed from current evidence each run.
- A retired cluster whose members scattered to several clusters follows its minting record.
  The other members' destinations are not represented; this is rare and a redirect target for
  such an id is a guess either way.
- A survivor that is not a public profile is held back until it is; if that happens after gp-api's
  cursor has passed the row's `retired_at`, gp-api sees it only on a full re-read.
- A split (a retired id becoming a minter again) drops its row. The id is live again and Person
  serves it directly; the API's `by-id` lookup never follows merges.
- Legacy full-uuid URLs, the gp-api poller, and the gp-marketing 307 to 308 change are app-side.

## Operating notes

- **Neither snapshot may be dropped or recreated.** `dbt build --full-refresh` leaves snapshots
  alone, but a manual drop loses the id history (and with it every forwarding row) or the slug
  history (every slug becomes null from then on). Neither may be made `appendOnly`: closing a row
  is an update.
- **Recovery.** Both snapshots keep 90 days of Delta history (a post_hook sets the file and log
  retention, because predictive optimization vacuums this schema on the default 7-day window). A
  bad run is undone with `RESTORE TABLE <snapshot> TO TIMESTAMP AS OF '<before the run>'`, which
  needs MODIFY. A dropped table comes back with `UNDROP TABLE <snapshot>` within the schema's
  recovery window, which needs the owner or MANAGE. Check the window before relying on it.
- **The Person sync refuses a swap when fewer than 90% of live ids survive.** A dedup pass that
  retires more than a tenth of Person in one run fails the whole swap set closed. Size the
  purge in batches or lower `min_id_overlap` for that run, deliberately.
- `PersonMerge` carries the same overlap floor because gp-api holds the retired ids, and a
  cold-start floor so an empty mart cannot publish an empty forwarding table.
