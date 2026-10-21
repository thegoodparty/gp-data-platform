"""DATA-2239 Area 2 build 3: gp_api user leftovers + creation-time campaign `details` keys.

Catalog item 3 (SOURCE_CATALOG 4.6). Everything here is product-side and 100%-reach on the user
table; the details keys are per-key coverage (58-92%). Pass-B rests on the "written once at
campaign creation" claim, which `audit_details_mutability.py` validates on the post-2026-04-13
CDC window — any key that audit shows mutating gets demoted to not-PIT before the family is
accepted.

Deliberately NOT built here, and why:
- `ballotLevel`: it is the DATA-2235 rebuild's election_level derivation source. One concept,
  one source — building it as a details feature would duplicate the rebuild's column.
- `hasPrimary`, `partisanType`, `tier`: BR-position duplicates (pos_has_primary, pos_is_partisan,
  pos_tier are already in the accepted BR family). Same concept from two systems is forbidden.
- `subAreaName/Value`, `electionCounty`, `priorElectionDates`-style race scaffolding presence:
  written by the product from the selected race, so "populated" encodes era + race-selection
  instrumentation, not candidate behavior — the coverage-proxy trap. (priorElectionDates presence
  collapsed 6,281 -> 115 across eras, which is the tell.)
- `avatar`, `meta_data` counters (sessionCount, lastVisited, profile_updated_count): mutable with
  no timestamp, pass A at best.
- `einNumber` / `validatedEin`: Pro-gated and post-signup; label-sharpener territory (Area 1).
- `email_domain_class`-covered shapes and `name_in_email`: already in v1 / the name family.

Contamination discipline (same as build_name_plausibility): the role-address and office-word
token lists and the area-code map are a-priori assets — public knowledge written down before any
label rate was computed, never grown by inspecting flagged rows. If a list must ever be
data-derived, derive it inside training folds or from pre-2025 rows only.

Intake decisions SETTLED 2026-08-12 (Tristan; full record in intake/gp_user_details_features.md
section 4): the model uses 12 columns. Excluded: usr_has_password + det_know_run_yes (one
auth-path/entry-channel latent, r=0.956 — "we're not learning about user behavior, just entry
channels"), det_pledged_* (no within-asked variance; reserve-marked for claims),
det_days_signup_to_product_deadline + missing indicator (BR deadline preferred; keep for
post-rebuild reconciliation), det_office_term_length_years (0.974 twin of BR
pos_election_frequency_years), usr_phone_toll_free_or_premium (rule candidate, 8 positives),
diag_mart_is_pledged (diagnostic). Ledger row "gp_user_details" 2026-08-12 (the later row
supersedes the earlier same-day one): +0.0028 pooled / +0.0039 decidable / +0.0164 forward.

Run: uv run --with pyarrow python projects/legit_candidate_predictor/build_gp_user_details.py
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
from intake import write_intake  # noqa: E402

# NANP area codes by state (public directory data, compiled a priori; unknown codes -> NaN
# rather than mismatch, so map gaps degrade to missing, never to a false signal).
AREA_CODES: dict[str, str] = {}
_BY_STATE = {
    "AL": "205 251 256 334 659 938",
    "AK": "907",
    "AZ": "480 520 602 623 928",
    "AR": "479 501 870",
    "CA": "209 213 279 310 323 341 350 408 415 424 442 510 530 559 562 619 626 628 650 657 "
    "661 669 707 714 747 760 805 818 820 831 840 858 909 916 925 949 951",
    "CO": "303 719 720 970 983",
    "CT": "203 475 860 959",
    "DE": "302",
    "DC": "202 771",
    "FL": "239 305 321 352 386 407 448 561 656 689 727 754 772 786 813 850 863 904 941 954",
    "GA": "229 404 470 478 678 706 762 770 912 943",
    "HI": "808",
    "ID": "208 986",
    "IL": "217 224 309 312 331 447 618 630 708 730 773 779 815 847 872",
    "IN": "219 260 317 463 574 765 812 930",
    "IA": "319 515 563 641 712",
    "KS": "316 620 785 913",
    "KY": "270 364 502 606 859",
    "LA": "225 318 337 504 985",
    "ME": "207",
    "MD": "240 301 410 443 667",
    "MA": "339 351 413 508 617 774 781 857 978",
    "MI": "231 248 269 313 517 586 616 679 734 810 906 947 989",
    "MN": "218 320 507 612 651 763 952",
    "MS": "228 601 662 769",
    "MO": "314 417 557 573 636 660 816",
    "MT": "406",
    "NE": "308 402 531",
    "NV": "702 725 775",
    "NH": "603",
    "NJ": "201 551 609 640 732 848 856 862 908 973",
    "NM": "505 575",
    "NY": "212 315 332 347 363 516 518 585 607 631 646 680 716 718 838 845 914 917 929 934",
    "NC": "252 336 704 743 828 910 919 980 984",
    "ND": "701",
    "OH": "216 220 234 326 330 380 419 440 513 567 614 740 937",
    "OK": "405 539 572 580 918",
    "OR": "458 503 541 971",
    "PA": "215 223 267 272 412 445 484 570 582 610 717 724 814 878",
    "RI": "401",
    "SC": "803 839 843 854 864",
    "SD": "605",
    "TN": "423 615 629 731 865 901 931",
    "TX": "210 214 254 281 325 346 361 409 430 432 469 512 682 713 726 737 806 817 830 832 "
    "903 915 936 940 945 956 972 979",
    "UT": "385 435 801",
    "VT": "802",
    "VA": "276 434 540 571 703 757 804 826 948",
    "WA": "206 253 360 425 509 564",
    "WV": "304 681",
    "WI": "262 274 414 534 608 715 920",
    "WY": "307",
}
for _st, _codes in _BY_STATE.items():
    for _c in _codes.split():
        AREA_CODES[_c] = _st

TOLL_FREE_OR_PREMIUM = {"800", "833", "844", "855", "866", "877", "888", "900"}

# Generic-mailbox local parts: an org inbox rather than a person.
ROLE_LOCAL_PARTS = {
    "info",
    "admin",
    "contact",
    "office",
    "hello",
    "team",
    "press",
    "media",
    "support",
    "mail",
    "campaign",
    "elect",
    "vote",
    "donate",
    "volunteer",
    "staff",
    "news",
}

# Office/campaign vocabulary anywhere in the email (local part or domain). Custom campaign
# domains (janedoeforcouncil.com) are the point, so the whole address is scanned.
OFFICE_WORDS = (
    "council",
    "mayor",
    "sheriff",
    "judge",
    "senate",
    "school",
    "schoolboard",
    "board",
    "commissioner",
    "trustee",
    "alderman",
    "supervisor",
    "clerk",
    "treasurer",
    "assessor",
    "coroner",
    "constable",
    "legislature",
    "assembly",
    "congress",
    "governor",
    "elect",
    "campaign",
    "vote",
    "4office",
    "foroffice",
)

SQL = """
with pop as (
  select u.user_id,
         cast(u.user_created_at as date) as signup_date,
         u.campaign_state,
         lower(trim(u.user_email))       as email,
         u.is_pledged                    as mart_is_pledged,
         cast(u.campaign_id as string)   as campaign_id
  from goodparty_data_catalog.mart_analytics.users_win_candidacy u
  where u.is_latest_version and not u.is_demo
),
ranked as (
  select p.*,
         row_number() over (partition by p.user_id order by c.created_at desc, p.campaign_id desc)
           as rn,
         c.details
  from pop p
  left join goodparty_data_catalog.dbt.stg_airbyte_source__gp_api_db_campaign c
    on cast(c.id as string) = p.campaign_id
)
select r.user_id,
       r.signup_date,
       r.campaign_state,
       r.email,
       r.mart_is_pledged,
       u.phone,
       u.has_password,
       det['knowRun']            as know_run,
       det['pledged']            as pledged,
       det['officeTermLength']   as office_term_length,
       det['filingPeriodsEnd']   as filing_periods_end,
       det['occupation']         as occupation,
       det['pastExperience']     as past_experience,
       det['website']            as website,
       det['runningAgainst']     as running_against,
       det['campaignCommittee']  as campaign_committee,
       det['customIssues']       as custom_issues,
       det['funFact']            as fun_fact
from (select *, from_json(details, 'map<string,string>') as det from ranked where rn = 1) r
left join goodparty_data_catalog.dbt.stg_airbyte_source__gp_api_db_user u
  on cast(u.id as string) = cast(r.user_id as string)
"""


def _area_code(phone: str | float | None) -> str | None:
    if not isinstance(phone, str):
        return None
    digits = re.sub(r"\D", "", phone)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits[:3] if len(digits) == 10 else None


def _nonempty(s: pd.Series) -> pd.Series:
    return (s.notna() & s.astype(str).str.strip().ne("") & s.astype(str).ne("null")).astype(int)


def build(raw: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame({"user_id": raw["user_id"]})

    # --- user table ---
    out["usr_has_password"] = raw["has_password"].fillna(False).astype(int)
    ac = raw["phone"].map(_area_code)
    out["usr_phone_present"] = ac.notna().astype(int)
    out["usr_phone_toll_free_or_premium"] = ac.isin(TOLL_FREE_OR_PREMIUM).astype(int)
    ac_state = ac.map(AREA_CODES)
    # NaN when there is no parseable phone or the code is not in the map; a real 0 means the
    # code maps to a different state than the declared one (relocation or virtual number).
    out["usr_phone_area_code_matches_state"] = np.where(
        ac_state.notna() & raw["campaign_state"].notna(),
        (ac_state == raw["campaign_state"]).astype(float),
        np.nan,
    )

    email = raw["email"].fillna("")
    local = email.str.split("@").str[0]
    out["usr_email_is_role_address"] = local.isin(ROLE_LOCAL_PARTS).astype(int)
    pat = "|".join(OFFICE_WORDS)
    out["usr_email_has_office_words"] = email.str.contains(pat, regex=True).astype(int)

    # --- creation-time details keys ---
    # knowRun is only ever 'yes' or absent (checked 2026-08-12: 13,277 yes / 47,625 null / no
    # other value), so this is "answered yes", not a yes/no split. The question was only IN the
    # onboarding flow during specific windows (monthly answer rate is ~0 outside Feb-Oct 2025 and
    # Jan-Mar 2026, including everything since Apr 2026), so a signup from an inactive month gets
    # NaN — "never asked" — rather than a 0 that conflates with "didn't answer". The gate is a
    # function of signup month only (label-free, knowable at signup, PIT-clean). Diagnostics
    # 2026-08-12: within active windows answered-yes corroborates 0.709 vs 0.441, and 0.661 vs
    # 0.397 among never-sales-touched users, so the signal is intent, not calendar; ungated, the
    # column imported calendar composition and dominated importances at 0.09.
    month = pd.to_datetime(raw["signup_date"]).dt.to_period("M")
    answered = raw["know_run"].eq("yes")
    month_rate = answered.groupby(month).mean()
    month_n = month.value_counts()
    active = set(month_rate[(month_rate >= 0.05) & (month_n >= 50)].index)
    out["det_know_run_yes"] = np.where(month.isin(active), answered.astype(float), np.nan)
    out["det_pledged_true"] = raw["pledged"].eq("true").astype(int)
    out["det_pledged_missing"] = raw["pledged"].isna().astype(int)

    out["det_office_term_length_years"] = (
        raw["office_term_length"].astype(str).str.extract(r"^(\d+)")[0].astype(float)
    )

    fpe = pd.to_datetime(raw["filing_periods_end"].str[:10], errors="coerce")
    signup = pd.to_datetime(raw["signup_date"])
    out["det_days_signup_to_product_deadline"] = (fpe - signup).dt.days
    out["det_product_deadline_missing"] = fpe.isna().astype(int)

    # Self-report effort: free-text fields only the user can fill. Populated = effort spent,
    # which is candidate behavior, not pipeline attention.
    selfreport = [
        "occupation",
        "past_experience",
        "website",
        "running_against",
        "campaign_committee",
        "custom_issues",
        "fun_fact",
    ]
    for c in selfreport:
        out[f"det_has_{c}"] = _nonempty(raw[c])
    out["det_n_selfreport_keys"] = out[[f"det_has_{c}" for c in selfreport]].sum(axis=1)

    # Diagnostic only, never a model feature: mart flag for the pledged agreement check.
    out["diag_mart_is_pledged"] = raw["mart_is_pledged"].astype("boolean").astype("Float64")
    return out


if __name__ == "__main__":
    raw = run_query(SQL)
    print(f"users: {len(raw):,}")
    assert raw["user_id"].is_unique, "one row per user expected"

    feats = build(raw)
    n_feat = feats.shape[1] - 2  # minus key + diagnostic
    print(f"features: {n_feat}")
    print(f"  has_password:        {feats.usr_has_password.mean():.1%}")
    print(
        f"  area-code matches:   {feats.usr_phone_area_code_matches_state.mean():.1%} "
        f"(known for {feats.usr_phone_area_code_matches_state.notna().mean():.1%})"
    )
    print(f"  role address:        {feats.usr_email_is_role_address.mean():.2%}")
    print(f"  office words:        {feats.usr_email_has_office_words.mean():.2%}")
    print(
        f"  knowRun yes:         {feats.det_know_run_yes.mean():.1%} of asked "
        f"(asked: {feats.det_know_run_yes.notna().mean():.1%})"
    )
    print(f"  pledged true:        {feats.det_pledged_true.mean():.1%}")
    print(f"  product deadline:    {1 - feats.det_product_deadline_missing.mean():.1%}")
    agree = (feats.det_pledged_true == feats.diag_mart_is_pledged.fillna(0)).mean()
    print(f"  pledged vs mart_is_pledged agreement: {agree:.1%}")

    out = HERE / "datasets" / "gp_user_details_features.parquet"
    feats.to_parquet(out, index=False)
    print(f"saved -> {out}")

    write_profile(
        feats,
        "gp_user_details_features",
        note="gp_api user leftovers + creation-time campaign details keys. Pass-B "
        "pending the details-mutability audit; det_days_signup_to_product_deadline "
        "admission is an open one-concept-one-source call vs the BR deadline.",
    )
    write_intake(
        feats,
        "gp_user_details_features",
        key="user_id",
        source="dbt.stg_airbyte_source__gp_api_db_user + gp_api_db_campaign.details "
        "(latest campaign per user)",
        join="user_id -> user table; campaign_id -> details map, row_number=1 by " "campaign created_at desc",
        pit_class="B claimed (creation-time writes); validated by "
        "audit_details_mutability.py on the post-2026-04-13 CDC window",
    )
