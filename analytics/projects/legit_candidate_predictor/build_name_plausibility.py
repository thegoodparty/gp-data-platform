"""DATA-2239 Area 2: name plausibility features.

"Is this even a person?" The model's existing name features are purely syntactic (token
count, length, has-digit, has-non-alpha, single-char-token). None of them asks whether the
name is a name. The 218M-row voter file answers that empirically, and it needs only name
frequency, not a person match, so this build is independent of the L2 / matcha decision.

Measured discrimination against the 59.4% base rate (probe_join_coverage.py
name_plausibility + name_patterns, 2026-08-06):
  neither name occurs in the voter file  n=48    4.2%
  first name == last name                n=24    8.3%
  tripled character in a name            n=11   18.2%
  last name unknown                      n=359  37.3%
  first name unknown                     n=219  40.6%
  placeholder token                      n=36   44.4%
  first-last pair never seen nationwide  n=2729 50.9%
  both names common                      n=18078 60.7%

Every bucket is small, so this family is expected to move ROC-AUC very little. Its job is
precision at the bottom of the score and an explanation a stakeholder will accept.

## Contamination discipline (read before editing the lists)

Three different things live in this file and only one of them is at risk.

1. Frequency features are computed from the voter file, which has no idea which of our
   signups corroborated. Label-free, and PIT-safe: how common a name is nationally is not a
   consequence of running for office. Clean.
2. `PLACEHOLDER_TOKENS` and `FICTIONAL_NAMES` are a-priori assets, written from general
   knowledge before any label rate was computed, never by inspecting our own users' names.
   That is what keeps them honest, and it is the property that must be preserved.
3. Feature *selection* is the real exposure. The `report()` output below is a DIAGNOSTIC, not
   a selection tool. Dropping or keeping a flag because of its corroboration rate on the full
   labelled set is selection on the outcome and it inflates every CV number downstream.

So: do NOT grow these lists by eyeballing flagged rows that failed to corroborate. That is
the direct-leakage version and it will not generalise forward. If a list ever needs to be
data-derived rather than a-priori, derive it inside the training folds only, or from pre-2025
rows, and validate on the frozen forward split.

Outputs datasets/name_features.parquet at user_id grain. Names and emails are NOT written
to the output; only derived features leave this script.

Run: uv run --with pyarrow python projects/legit_candidate_predictor/build_name_plausibility.py
"""

import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1] / "lib"))
from profile import write_profile  # noqa: E402

from databricks_conn import run_query  # noqa: E402

# Frequency of each candidate's first name, last name, and exact first-last pair in the
# nationwide voter file. Computed for every Win user so the output covers both working sets.
SQL = """
with u as (
  select distinct
    user_id,
    upper(trim(user_first_name)) as fn,
    upper(trim(user_last_name))  as ln,
    lower(trim(user_email))      as email
  from goodparty_data_catalog.mart_analytics.users_win_candidacy
  where is_latest_version and not is_demo
),
fn_freq as (
  select upper(trim(Voters_FirstName)) as fn, count(*) as n_fn
  from goodparty_data_catalog.dbt.int__l2_nationwide_uniform
  where Voters_FirstName is not null
  group by 1
),
ln_freq as (
  select upper(trim(Voters_LastName)) as ln, count(*) as n_ln
  from goodparty_data_catalog.dbt.int__l2_nationwide_uniform
  where Voters_LastName is not null
  group by 1
),
pair_freq as (
  select upper(trim(Voters_FirstName)) as fn, upper(trim(Voters_LastName)) as ln,
         count(*) as n_pair
  from goodparty_data_catalog.dbt.int__l2_nationwide_uniform
  where Voters_FirstName is not null and Voters_LastName is not null
  group by 1, 2
)
select u.user_id, u.fn, u.ln, u.email,
       coalesce(f.n_fn, 0)   as n_fn,
       coalesce(l.n_ln, 0)   as n_ln,
       coalesce(p.n_pair, 0) as n_pair
from u
left join fn_freq   f on f.fn = u.fn
left join ln_freq   l on l.ln = u.ln
left join pair_freq p on p.fn = u.fn and p.ln = u.ln
"""

# Token-level, not substring: a substring rule flags real surnames and measured worse
# (44% corroboration, i.e. false positives) in the probe.
PLACEHOLDER_TOKENS = {
    "test",
    "testing",
    "tester",
    "demo",
    "sample",
    "example",
    "asdf",
    "asdfg",
    "qwerty",
    "admin",
    "user",
    "none",
    "na",
    "null",
    "fake",
    "xxx",
    "xx",
    "abc",
    "aaa",
    "foo",
    "bar",
    "baz",
    "unknown",
    "anonymous",
    "noname",
    "firstname",
    "lastname",
    "tbd",
    "nope",
}

TRIPLED_CHAR = re.compile(r"(.)\1\1")

# Kept deliberately despite a bad-looking label rate (18 users, 66.7% corroborated vs 59.4%
# base). Pruning it on that basis would be feature selection on the full labelled set, which
# is exactly the contamination this family has to avoid. Its a-priori weakness is real and
# separately arguable (these letter runs occur inside ordinary surnames, so a substring test
# is poorly specified), but the decision belongs to the model, not to the label.
KEYBOARD_RUNS = (
    "asdf",
    "sdfg",
    "dfgh",
    "fghj",
    "ghjk",
    "hjkl",
    "qwer",
    "wert",
    "erty",
    "rtyu",
    "tyui",
    "yuio",
    "uiop",
    "zxcv",
    "xcvb",
    "cvbn",
    "vbnm",
    "wasd",
    "poiu",
    "lkjh",
    "mnbv",
    "1234",
    "2345",
)

# Frequency cannot catch these: both parts are ordinary names. Deliberately short, and it
# needs a periodic review to stay useful. This is a FEATURE, not a filter: some real people
# share these names, and the model is free to learn that the flag is weak.
FICTIONAL_NAMES = {
    "james bond",
    "john doe",
    "jane doe",
    "mickey mouse",
    "minnie mouse",
    "donald duck",
    "harry potter",
    "hermione granger",
    "luke skywalker",
    "darth vader",
    "han solo",
    "clark kent",
    "bruce wayne",
    "peter parker",
    "tony stark",
    "steve rogers",
    "sherlock holmes",
    "homer simpson",
    "bart simpson",
    "marge simpson",
    "lisa simpson",
    "spongebob squarepants",
    "ronald mcdonald",
    "forrest gump",
    "jack sparrow",
    "tyler durden",
    "walter white",
    "jesse pinkman",
    "indiana jones",
    "john wick",
    "frodo baggins",
    "bilbo baggins",
    "tony soprano",
    "don draper",
    "michael scott",
    "dwight schrute",
    "ron swanson",
    "leslie knope",
    "rick sanchez",
    "morty smith",
    "peter griffin",
    "stewie griffin",
    "eric cartman",
    "ash ketchum",
    "lara croft",
    "john rambo",
    "ellen ripley",
    "santa claus",
    "jesus christ",
    "elvis presley",
    "michael jackson",
    "chuck norris",
    "donald trump",
    "joe biden",
    "barack obama",
    "kamala harris",
    "hillary clinton",
    "elon musk",
    "taylor swift",
    "lebron james",
    "tom brady",
    "george washington",
    "abraham lincoln",
}


def build(df: pd.DataFrame) -> pd.DataFrame:
    fn = df["fn"].fillna("").str.strip()
    ln = df["ln"].fillna("").str.strip()
    full = (fn + " " + ln).str.strip().str.lower()

    out = pd.DataFrame({"user_id": df["user_id"]})

    # Frequency, the empirically strongest part of the family. log1p keeps the long tail usable.
    out["name_first_freq_log"] = np.log1p(df["n_fn"])
    out["name_last_freq_log"] = np.log1p(df["n_ln"])
    out["name_pair_freq_log"] = np.log1p(df["n_pair"])
    out["name_first_unknown"] = df["n_fn"].eq(0).astype(int)
    out["name_last_unknown"] = df["n_ln"].eq(0).astype(int)
    out["name_both_unknown"] = (df["n_fn"].eq(0) & df["n_ln"].eq(0)).astype(int)
    out["name_pair_unseen"] = df["n_pair"].eq(0).astype(int)

    # Structural implausibility.
    out["name_fn_eq_ln"] = (fn.str.lower().eq(ln.str.lower()) & fn.ne("")).astype(int)
    out["name_tripled_char"] = [
        int(bool(TRIPLED_CHAR.search(a)) or bool(TRIPLED_CHAR.search(b))) for a, b in zip(fn, ln, strict=True)
    ]
    out["name_token_too_short"] = (fn.str.len().le(2) | ln.str.len().le(2)).astype(int)

    tokens = (fn.str.lower() + " " + ln.str.lower()).str.split()
    out["name_placeholder_token"] = tokens.apply(
        lambda ts: int(any(t.strip(".,'-") in PLACEHOLDER_TOKENS for t in ts))
    )
    low = fn.str.lower() + ln.str.lower()
    out["name_keyboard_run"] = low.apply(lambda s: int(any(r in s for r in KEYBOARD_RUNS))).astype(int)
    out["name_fictional"] = full.isin(FICTIONAL_NAMES).astype(int)

    # Positive plausibility: the email was built from the name.
    local = df["email"].fillna("").str.split("@").str[0].str.replace(r"[^a-z]", "", regex=True)
    fn_l, ln_l = fn.str.lower(), ln.str.lower()
    out["name_in_email"] = [
        int(bool(f) and bool(last) and (f in loc or last in loc))
        for f, last, loc in zip(fn_l, ln_l, local, strict=True)
    ]
    return out


def report(feats: pd.DataFrame) -> None:
    """DIAGNOSTIC ONLY. Corroboration rate per flag, for the ledger note.

    Never use this output to add, drop, or tune a flag: that is selection on the outcome
    using the same labels the folds will be scored against. See the module docstring.
    """
    ws = HERE / "datasets" / "working_set.parquet"
    if not ws.exists():
        print("(no working_set.parquet; skipping label diagnostics)")
        return
    lab = pd.read_parquet(ws, columns=["user_id", "corroborated"]).drop_duplicates("user_id")
    m = feats.merge(lab, on="user_id", how="inner")
    base = m["corroborated"].mean()
    print(f"\nlabelled overlap: {len(m):,} users, base corroboration {base:.1%}")
    flags = [c for c in feats.columns if c.startswith("name_") and feats[c].isin([0, 1]).all()]
    rows = []
    for c in flags:
        hit = m[m[c].eq(1)]
        if len(hit):
            rows.append((c, len(hit), hit["corroborated"].mean(), hit["corroborated"].mean() - base))
    rep = pd.DataFrame(rows, columns=["flag", "n", "corroboration", "vs_base"])
    print(
        rep.sort_values("corroboration").to_string(
            index=False, formatters={"corroboration": "{:.1%}".format, "vs_base": "{:+.1%}".format}
        )
    )


if __name__ == "__main__":
    raw = run_query(SQL)
    print(
        f"users: {len(raw):,}  first-name known: {raw.n_fn.gt(0).mean():.1%}  "
        f"last-name known: {raw.n_ln.gt(0).mean():.1%}  pair seen: {raw.n_pair.gt(0).mean():.1%}"
    )

    feats = build(raw)
    assert feats["user_id"].is_unique, "name features must be one row per user"
    report(feats)

    out = HERE / "datasets" / "name_features.parquet"
    feats.to_parquet(out, index=False)
    print(f"\nsaved -> {out}  ({feats.shape[1] - 1} features)")

    # Every new dataset ships an eyeball-able profile (Tristan, 2026-08-06), not just a parquet.
    write_profile(
        feats,
        "name_features",
        note="Voter-file name frequency plus a-priori structural flags, user grain. Flag rates "
        "against the label are in SESSION_NOTES; they are diagnostics and must not be used "
        "to prune flags.",
    )
