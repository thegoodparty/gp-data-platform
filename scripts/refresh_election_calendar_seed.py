"""Refresh dbt/project/seeds/election_calendar.csv from L2 (past cycles, where an
election has actually happened) and BallotReady (current/future cycles, where L2
has no ground truth yet -- it's retrospective-only).

NOTE on this script's role: m_election_api__election_calendar treats
int__election_calendar_primary_ballotready (a live dbt model implementing the
same rule as this script, recomputed every run) as authoritative for Primary --
it overrides this seed's Primary rows automatically whenever it has an answer,
including for BallotReady-sourced rows this script previously added. So running
this script is no longer required to keep Primary rows current; the live model
already does that continuously. What this script is still for:
  1. Backfilling L2-confirmed rows once a cycle's election has actually
     happened -- L2 ground truth doesn't come from BallotReady at all, so the
     live model can't produce these on its own.
  2. Populating the seed's fallback rows for a (state, year) the live model
     currently excludes (e.g. CA -- see the live model's header comment), so
     there's still a reasonable answer for that gap until it's resolved
     upstream.
  3. Keeping the checked-in seed a comprehensive, human-readable snapshot of
     everything known, even for rows the live model would also compute.

General rows are always computed: the Tuesday-after-first-Monday-in-November rule
is a fixed federal statute that has not changed and is not expected to. This
script does not touch General rows; it only fills in Primary rows for years/states
not yet in the seed.

## BallotReady Primary-day rule

Filter BallotReady's election table (goodparty_data_catalog.dbt.
stg_airbyte_source__ballotready_api_election) to elections that are plausibly
each state's Primary day:

    name LIKE '%Primary%'
      AND name NOT LIKE '%Runoff%'
      AND name NOT LIKE '%Consolidated%'
      AND name NOT LIKE '%Special%'
      AND is_special = false
      AND race_count > 0

grouped by (state, year). This can still leave more than one candidate row per
group:

- **Wisconsin is a named, permanent exception, not a heuristic.** Wisconsin
  statute runs two structurally distinct primaries every even year: a
  nonpartisan spring primary (Feb, for local/judicial/school-board races that
  drew 3+ candidates -- Wis. Stat. secs. 5.02(21), 8.05) and a partisan primary
  for state and federal partisan office (Aug, Wis. Stat. sec. 8.15). Both
  elections are literally named "Wisconsin Primary Election" in BallotReady's
  data with no other text distinguishing them, and the spring one structurally
  covers far more seats (thousands of races) than the partisan one (a few
  hundred) -- so for every other state "pick the row with the most races" is
  the right disambiguator, but for Wisconsin specifically it picks the wrong
  one. Confirmed against 3 independently L2-verified cycles (2020, 2022, 2024)
  before treating this as a rule rather than a coincidence: the fewer-races
  (August, partisan) row matched L2's dominant Primary_YYYY_MM_DD column in
  all 3. Do not "fix" this by switching to a generic tie-break (e.g. closest to
  the November general) -- that was tried and tested worse overall (see
  ARCHIVE below), because it silently breaks AR/MS/NC, where a small, spurious
  same-named row falls later in the year than the true statewide primary.
- **Every other state:** the row with the most races wins.
- **If the filter above finds zero rows** for a (state, year) -- confirmed to
  happen for DC in 2024, where BallotReady split that cycle's primary into
  party-specific "Republican/Democratic Presidential Primary Election" rows
  with no plain "Primary Election" row at all -- retry including
  Presidential-named rows, same most-races-wins logic. This is a generic
  fallback, not a DC-specific hardcode: DC's other cycles (2018-2030) all have
  a single plain "Primary Election" row and never hit this branch.

Validated against every (state, year) pair with confirmed L2 ground truth
(2024, all 50 available states; 2026, the 6 states that had already voted as
of this script's authoring): 55/55 exact date matches.

ARCHIVE -- alternatives tried and rejected before landing on the above:
- max(race_count) with no Wisconsin exception: 53/55 (fails WI both cycles it
  has two candidates).
- closest-to-the-November-general as the general tie-break (replacing
  max(race_count) everywhere, not just for ties): 51/55 -- correctly flips
  Wisconsin, but wrongly flips AR/MS/NC, which were already correct under
  max(race_count).

## Known gaps this script does not fix

- CA is absent from the seed entirely: its L2 vote-history staging view
  (stg_dbt_source__l2_s3_ca_vote_history) currently fails with an
  unresolved-column error, so there was no ground truth to validate the
  BallotReady rule against for CA before trusting it there. Once that dbt-
  owned view is fixed, validate CA specifically before adding it.
- This script only fills in years present in TARGET_YEARS below. Extend that
  list each cycle (or pass a different set) as new years become relevant.

Requires env vars: DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN
"""

import csv
import os

from databricks import sql

# Requires env vars: DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN
# (same DATABRICKS_HOST/DATABRICKS_TOKEN convention as scripts/promote_viability_models.py;
# DATABRICKS_HTTP_PATH is additional -- this script goes through a SQL warehouse,
# not the mlflow tracking URI that script uses).
DATABRICKS_HOST = os.environ["DATABRICKS_HOST"].removeprefix("https://").removeprefix("http://")
DATABRICKS_HTTP_PATH = os.environ["DATABRICKS_HTTP_PATH"]
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]

SEED_PATH = "dbt/project/seeds/election_calendar.csv"

# Extend this each cycle. BallotReady already has scheduled elections through at
# least 2030 at the time this script was written, so it's safe to run ahead of
# the years L2 can confirm -- that's the whole point (L2 is retrospective-only).
TARGET_YEARS = (2024, 2026)

WISCONSIN_FEWER_RACES_WINS = {"WI"}

# CA's L2 vote-history staging view is currently broken (unresolved-column
# error), so the BallotReady rule above was never validated against CA -- no
# ground truth existed to check it against. Excluded here so a future run of
# this script doesn't silently add an unvalidated CA row. Remove from this set
# once that view is fixed and CA is checked against real L2 data like every
# other state was.
NOT_YET_VALIDATED_STATES = {"CA"}

PRIMARY_FILTER_SQL = """
    name LIKE '%Primary%'
    AND name NOT LIKE '%Runoff%'
    AND name NOT LIKE '%Consolidated%'
    AND name NOT LIKE '%Special%'
    AND is_special = false
    AND race_count > 0
"""


def _fetch_candidates(cur, include_presidential: bool):
    presidential_excl = "" if include_presidential else "AND name NOT LIKE '%Presidential%'"
    excluded_states_sql = ", ".join(f"'{s}'" for s in NOT_YET_VALIDATED_STATES)
    cur.execute(f"""
        SELECT state, election_day, race_count, name, YEAR(election_day) AS yr
        FROM goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_election
        WHERE {PRIMARY_FILTER_SQL}
          {presidential_excl}
          AND state NOT IN ({excluded_states_sql})
          AND YEAR(election_day) IN ({','.join(str(y) for y in TARGET_YEARS)})
    """)
    return cur.fetchall()


def _race_count(row):
    return row[2]


def _pick(rows, state):
    if state in WISCONSIN_FEWER_RACES_WINS:
        return min(rows, key=_race_count)
    return max(rows, key=_race_count)


def resolve_primary_dates(cur):
    """(state, year) -> (election_date, race_count, name) for every resolvable pair."""
    primary_rows = _fetch_candidates(cur, include_presidential=False)
    fallback_rows = _fetch_candidates(cur, include_presidential=True)

    by_key, fallback_by_key = {}, {}
    for r in primary_rows:
        by_key.setdefault((r[0], r[4]), []).append(r)
    for r in fallback_rows:
        fallback_by_key.setdefault((r[0], r[4]), []).append(r)

    resolved = {}
    for key, rows in by_key.items():
        resolved[key] = _pick(rows, key[0])
    for key, rows in fallback_by_key.items():
        if key not in resolved:  # only the zero-match fallback path
            resolved[key] = _pick(rows, key[0])
    return resolved


def main():
    with open(SEED_PATH, newline="") as f:
        existing = list(csv.DictReader(f))
    existing_keys = {(r["state"], r["election_date"]) for r in existing}

    conn = sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    )
    cur = conn.cursor()
    resolved = resolve_primary_dates(cur)
    cur.close()
    conn.close()

    new_rows = []
    for (state, _year), (_state, election_day, race_count, name, _yr) in resolved.items():
        date_str = election_day.isoformat()
        if (state, date_str) in existing_keys:
            continue  # already have this one (from L2 or a prior run)
        new_rows.append(
            {
                "state": state,
                "election_date": date_str,
                "election_code": "Primary",
                "source": "ballotready",
                "source_note": f"BallotReady '{name}' ({race_count:,} races)",
            }
        )

    all_rows = existing + new_rows
    all_rows.sort(key=lambda r: (r["state"], r["election_date"]))

    with open(SEED_PATH, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["state", "election_date", "election_code", "source", "source_note"])
        w.writeheader()
        w.writerows(all_rows)

    print(f"{len(new_rows)} new rows added, {len(all_rows)} total rows in {SEED_PATH}")


if __name__ == "__main__":
    main()
