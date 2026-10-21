"""DATA-2225: signup-time geo-consistency features.

"Is the person accessing from the state they're running in?" (Tristan 2026-08-05).

Constraints (discovered 2026-08-05, corrected 2026-08-06):
- Amplitude `location_lat/lng` are NEVER populated in our stream, so distance-in-km
  is not buildable from this source; state-level match flags are the substitute.
- Geo (region/country) is IP-derived, so SERVER-EMITTED events (session_id=-1, up to
  ~42% of the stream in some months) carry none, and the geo columns exist only from
  2025-04-24. Features are therefore taken from the user's earliest geo-bearing event
  WITHIN [signup-1d, signup+30d] (v1 wrongly required the user's first-EVER geo event
  to fall in that window, which failed anyone with pre-signup anonymous browsing).

Outputs datasets/geo_features.parquet (user_id grain):
- zip_campaign_state_match  stated zip's state == declared campaign_state (product data,
                            signup-tier, ~full coverage; NaN when zip invalid)
- geo_state_match           login-region state == campaign_state (NaN unknown)
- geo_zip_state_match       login-region state == stated zip's state (NaN unknown)
- geo_country_nonus         near-signup login outside the US
- geo_missing               1 = no geo-bearing event within the signup window

Run:  uv run --with pyarrow --with zipcodes python projects/legit_candidate_predictor/build_geo.py
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "lib"))
from databricks_conn import run_query  # noqa: E402

STATE_TO_CODE = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
    "District of Columbia": "DC",
    "Puerto Rico": "PR",
    "Guam": "GU",
    "Virgin Islands": "VI",
    "American Samoa": "AS",
    "Northern Mariana Islands": "MP",
}

GEO_SQL = """
with win_users as (
  select user_id, min(cast(user_created_at as date)) as signup_date
  from goodparty_data_catalog.mart_analytics.users_win_candidacy
  where is_latest_version
  group by user_id
),
ev as (
  select
    try_cast(e.user_id as bigint) as user_id,
    cast(e.event_time as date)    as d,
    e.region,
    e.country,
    row_number() over (
      partition by try_cast(e.user_id as bigint) order by e.event_time
    ) as rn
  from goodparty_data_catalog.dbt.stg_airbyte_source__amplitude_api_events e
  join win_users w
    on w.user_id = try_cast(e.user_id as bigint)
   and cast(e.event_time as date)
       between date_add(w.signup_date, -1) and date_add(w.signup_date, 30)
  where try_cast(e.user_id as bigint) is not null
    and e.region is not null
)
select user_id, d as first_geo_date, region, country
from ev where rn = 1
"""

GATE_DAYS = 30


def zip_states(zips: pd.Series) -> dict:
    import zipcodes  # bundled data, offline

    out = {}
    for z in zips.dropna().astype(str).str.strip().str[:5].unique():
        if not (len(z) == 5 and z.isdigit()):
            continue
        try:
            m = zipcodes.matching(z)
        except Exception:
            continue
        if m and m[0].get("state"):
            out[z] = m[0]["state"]
    return out


if __name__ == "__main__":
    # Every population that will ever be fed to the model, training and scoring alike.
    # `scoring_population.parquet` was added 2026-08-14: geo is a BASELINE feature block, so
    # a user missing from here scores with geo_missing=1 and four NaNs. Building it for the
    # training rows only would have made that the normal state of every live row — a
    # train/serve skew invisible in any training metric.
    POOLS = ("working_set.parquet", "working_set_2024.parquet", "scoring_population.parquet")
    frames = []
    for name in POOLS:
        path = HERE / "datasets" / name
        if not path.exists():
            print(f"  skipping {name} (absent)")
            continue
        frames.append(pd.read_parquet(path, columns=["user_id", "user_zip", "campaign_state", "signup_date"]))
        print(f"  {name}: {len(frames[-1]):,} rows")
    users = pd.concat(frames, ignore_index=True).drop_duplicates("user_id")
    print(f"users needing geo: {len(users):,}")

    geo = run_query(GEO_SQL)
    print(f"users with a geo-bearing amplitude event: {len(geo):,}")

    df = users.merge(geo, on="user_id", how="left")

    # the SQL already restricts to events within [signup-1d, signup+GATE_DAYS];
    # a row present = geo observed near signup
    gated = df["region"].notna()
    print(f"geo within {GATE_DAYS}d of signup: {gated.mean():.1%} of pool")

    login_state = df["region"].map(STATE_TO_CODE).where(gated)

    zmap = zip_states(df["user_zip"])
    z5 = df["user_zip"].astype(str).str.strip().str[:5]
    zip_state = z5.map(zmap)

    out = pd.DataFrame({"user_id": df["user_id"]})
    out["zip_campaign_state_match"] = np.where(
        zip_state.notna() & df["campaign_state"].notna(),
        (zip_state == df["campaign_state"]).astype(float),
        np.nan,
    )
    out["geo_state_match"] = np.where(
        login_state.notna() & df["campaign_state"].notna(),
        (login_state == df["campaign_state"]).astype(float),
        np.nan,
    )
    out["geo_zip_state_match"] = np.where(
        login_state.notna() & zip_state.notna(), (login_state == zip_state).astype(float), np.nan
    )
    out["geo_country_nonus"] = np.where(
        gated & df["country"].notna(), df["country"].ne("United States").astype(float), np.nan
    )
    out["geo_missing"] = (~gated).astype(int)

    out.to_parquet(HERE / "datasets" / "geo_features.parquet", index=False)
    print(
        f"zip valid: {zip_state.notna().mean():.1%}  "
        f"zip==campaign_state: {out.zip_campaign_state_match.mean():.1%} (where known)"
    )
    print(
        f"login==campaign_state: {out.geo_state_match.mean():.1%} (where known, n={out.geo_state_match.notna().sum():,})"
    )
    print(f"login==zip state:      {out.geo_zip_state_match.mean():.1%} (where known)")
    print(f"saved -> {HERE / 'datasets' / 'geo_features.parquet'}")
