#!/usr/bin/env python3
"""Aggregate ddhq-miss-audit results into the candidacy_stage-correct decomposition
and write the internal detail CSV + the external aggregate summary + winners list.

Usage:
  python aggregate.py <work_dir>

<work_dir> must contain:
  records.json     -- all unmatched records w/ deterministic `category` and `office_level`
                      (the classify.sql output)
  all_verified.json-- merged web-verification results (one object per C1/C2/C3b record)

The C3 records (same name+state present in DDHQ at a different date) are NOT
web-verified; they are folded in from records.json by their deterministic category.
classify.sql matches people on name+state only, so C3 is an UPPER BOUND on "we already
hold this candidacy elsewhere" -- some are namesakes. This is safe for the headline:
the matcher-FN signal lives entirely in C3b (same name+state+DATE), which IS verified,
and a different-date C3 can never hide a same-date FN.

COVERAGE IS A SIGNAL, NOT GROUND TRUTH. Whether DDHQ has the race is pre-sorted by the
deterministic SQL via a coarse locality-name overlap (C1 = no locality match in DDHQ;
C2/C3b = DDHQ holds a race in the locality), then refined by web verification. We
contract DDHQ to cover small races, so do NOT infer coverage from office size. The
subagents only report what happened (ran / outcome / actual office / office level);
they do not guess whether DDHQ "should" cover the race.
"""

import csv
import json
import re
import sys
from collections import Counter

work = sys.argv[1]
allrec = {r["unique_id"]: r for r in json.load(open(f"{work}/records.json"))}
ver = {v["unique_id"]: v for v in json.load(open(f"{work}/all_verified.json"))}

# Optional AI office-judgment pass (office_judgments.json). For every C2/C3b record
# whose subagent reported a non-empty actual_office, a small model judged whether that
# office is the SAME seat as ours (just rephrased) or GENUINELY DIFFERENT. This is the
# primary signal for the product-wrong-office bucket; the token heuristic below is only
# a fallback for records the judge pass did not cover. See SKILL.md step 3b.
try:
    office_judgment = {
        j["unique_id"]: j["judgment"] for j in json.load(open(f"{work}/office_judgments.json"))
    }
except FileNotFoundError:
    office_judgment = {}

# Tokens that carry no office-identity meaning when comparing two office strings.
# Subagents routinely rephrase our office ("Town Commission" -> "Town Commissioner",
# "District 1" -> "Seat 1", "School Board" -> "School District board"); those are the
# SAME office and must NOT be counted as a product wrong-office error.
_OFFICE_NOISE = {
    "the",
    "of",
    "for",
    "and",
    "a",
    "an",
    "at",
    "no",
    "number",
    "district",
    "seat",
    "place",
    "ward",
    "zone",
    "position",
    "subdistrict",
    "writein",
    "write",
    "in",
    "general",
    "special",
    "election",
    "area",
    "large",
}
# Equivalence classes folded to one token before comparison.
_OFFICE_SYNONYM = {
    "commissioner": "commission",
    "councilman": "council",
    "councilwoman": "council",
    "councilmember": "council",
    "councilor": "council",
    "aldermanic": "alderman",
    "directors": "director",
    "board": "board",
    "boardofeducation": "school",
    "schools": "school",
}


def _office_tokens(s):
    s = re.sub(r"[^a-z0-9]+", " ", (s or "").lower())
    # keep numeric tokens of any length: a 1-2 digit seat/district number is
    # office-identity-bearing ("district 1" vs "district 2" are different seats).
    toks = [_OFFICE_SYNONYM.get(t, t) for t in s.split() if len(t) >= 3 or t.isdigit()]
    return {t for t in toks if t not in _OFFICE_NOISE}


def same_office(uid, ours, actual):
    """True when `actual` (what the subagent reported) is just a rephrasing of `ours`,
    not a genuinely different office, so it is NOT a product wrong-office error. Prefers
    the AI office-judgment pass; falls back to a token heuristic when no judgment exists."""
    j = office_judgment.get(uid)
    if j == "same":
        return True
    if j in ("different", "unsure"):  # genuinely different (or judge unsure -> keep flagged)
        return False
    a, b = _office_tokens(ours), _office_tokens(actual)
    if not a or not b:
        return False
    if a <= b or b <= a:  # one is a subset of the other
        return True
    return len(a & b) / len(a | b) >= 0.6


def bucket(uid):
    """Re-bucket under candidacy_stage (person + office + election_date). Coverage is
    taken from the deterministic SQL category, never from an office-size guess."""
    dc = allrec[uid]["category"]
    if dc.startswith("C3_singleton"):  # same name+state in DDHQ, different date
        return "prod_date"
    v = ver.get(uid)
    if v is None:
        return "other"
    fc = v.get("final_category", "")
    ao = (v.get("actual_office") or "").strip()
    # candidacy status from the subagent (scheme-tolerant: accepts old or new labels)
    if fc in ("non_candidate", "NON_CANDIDATE"):
        return "no_candidacy"
    if fc in ("C2_not_on_ballot", "NOT_ON_BALLOT"):
        return "not_on_ballot"
    # the person did run. coverage comes from the SQL category:
    if dc in ("C1_race_not_tracked", "C1_absent_for_state"):
        return "race_not_in_ddhq"  # race absent from DDHQ data (any office detail moot)
    # dc is C2 / C3b -> the race IS present in DDHQ
    if dc == "C3b_person_sameday_nearFN":  # same name+state+date that didn't cluster
        # The only genuine matcher FN. Takes precedence over an office rephrasing so a
        # reworded actual_office can never steal it into prod_office and understate the
        # headline FN count.
        return "matcher_fn"
    if ao and not same_office(uid, allrec[uid]["office"], ao):
        return "prod_office"  # we stored a genuinely different office for a DDHQ race
    return "ddhq_present_person_absent"  # race in DDHQ, candidate absent from its results


b = Counter(bucket(u) for u in allrec)
total = sum(b.values())


def pct(n):
    # guard the empty-input case (wrong window / no misses) so the CSVs still write
    return f"{100 * n / total:.1f}%" if total else "n/a"


LABELS = [
    (
        "no_candidacy",
        "Never ran / no evidence of a campaign",
        "Yes",
        "Signed up but no evidence of a real candidacy anywhere in the state.",
    ),
    (
        "race_not_in_ddhq",
        "Real candidate; race not present in DDHQ data",
        "Yes",
        "Genuine candidate, but the race does not appear in DDHQ at any date (not loaded or not contracted). See office-level breakdown below. Includes confirmed winners.",
    ),
    (
        "not_on_ballot",
        "Filed or explored, never appeared on the ballot",
        "Yes",
        "Declared, filed, or campaigned but withdrew or missed filing.",
    ),
    (
        "prod_office",
        "Product record had the wrong office",
        "Yes (as recorded)",
        "The race is in DDHQ, but our product stored a different office than the one the person actually ran for.",
    ),
    (
        "prod_date",
        "Product record had the wrong or single election date",
        "Yes (as recorded)",
        "We appear to already hold this candidate in DDHQ at a different date; our product stored a different (often placeholder) date.",
    ),
    (
        "ddhq_present_person_absent",
        "Race is in DDHQ but the candidate is absent from its results",
        "Mostly",
        "On the ballot in a race DDHQ has, but absent from the reported results (DDHQ often lists only winners in minor races).",
    ),
    (
        "matcher_fn",
        "Possible true results-match miss (under review)",
        "No",
        "Same person, office, and date appear in DDHQ but did not link. Flagged for manual review.",
    ),
]

# Records with no verification entry bucket to "other". That is a data gap (a batch
# result was dropped or never written), NOT a result. Surface it as its own row and
# warn loudly; never fold it into matcher_fn, the highest-severity bucket.
if b["other"]:
    print(
        f"WARNING: {b['other']} record(s) have no verification entry in all_verified.json. "
        "Listed as 'pending verification' in the summary, not folded into any outcome. "
        "Re-run the fan-out and merge all batch results before publishing.",
        file=sys.stderr,
    )

# external aggregate summary (no per-person labels)
with open(f"{work}/ddhq_unmatched_summary.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["outcome_category", "count", "share_of_unmatched", "expected_to_be_unmatched", "description"])
    for key, label, corr, desc in LABELS:
        n = b[key]
        w.writerow([label, n, pct(n), corr, desc])
    if b["other"]:
        w.writerow(
            [
                "Pending verification (data gap, not a result)",
                b["other"],
                pct(b["other"]),
                "unknown",
                "No verification entry — batch result(s) missing or failed. Do not publish until resolved.",
            ]
        )
    w.writerow(["Total unmatched", total, "100.0%" if total else "n/a", "", ""])

# office-level breakdown of real candidates whose race is not in DDHQ (audit aid)
lvl = Counter(allrec[u].get("office_level") or "Unknown" for u in allrec if bucket(u) == "race_not_in_ddhq")
with open(f"{work}/ddhq_unmatched_by_office_level.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["office_level", "real_candidates_race_not_in_ddhq"])
    for level, n in sorted(lvl.items(), key=lambda x: -x[1]):
        w.writerow([level, n])

# internal per-record detail
cols = [
    "unique_id",
    "fn",
    "ln",
    "state",
    "dt",
    "office",
    "office_level",
    "category",
    "final_category",
    "ran",
    "made_ballot",
    "result",
    "actual_office",
    "name_found_elsewhere_in_state",
    "confidence",
    "source_url",
    "notes",
]
with open(f"{work}/ddhq_unmatched_detail.csv", "w", newline="") as f:
    dw = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
    dw.writeheader()
    for uid, d in allrec.items():
        row = dict(d)
        row.update(ver.get(uid, {}))
        dw.writerow(row)


# confirmed winners (public record): web-verified wins (C1/C2/C3b) PLUS C3 singleton
# wins, which are skipped in web verification because we already hold the DDHQ result
# at a different date — they would otherwise never reach this list.
def _is_win(uid, d):
    v = ver.get(uid, {})
    # result and final_category are independent fields in the contract; trust either so a
    # WON / result=unknown mismatch is not silently dropped from the winners list.
    return v.get("result") == "won" or v.get("final_category") == "WON" or d["category"] == "C3_singleton_WON"


won_uids = sorted(
    (uid for uid, d in allrec.items() if _is_win(uid, d)),
    key=lambda uid: (allrec[uid]["state"], allrec[uid]["ln"]),
)
with open(f"{work}/ddhq_unmatched_confirmed_wins.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["candidate", "state", "office_won", "office_level", "race_in_ddhq", "confidence", "source"])
    for uid in won_uids:
        d = allrec[uid]
        v = ver.get(uid, {})
        held_in_ddhq = d["category"] == "C3_singleton_WON"  # win we already hold
        office = (v.get("actual_office") or d["office"]).strip()
        in_ddhq = "no" if d["category"].startswith("C1_") else "yes"
        w.writerow(
            [
                f"{d['fn']} {d['ln']}".title(),
                d["state"],
                office,
                d.get("office_level") or "",
                in_ddhq,
                v.get("confidence", "high" if held_in_ddhq else ""),
                v.get("source_url", "DDHQ (held at a different date)" if held_in_ddhq else ""),
            ]
        )

print(f"total unmatched: {total}")
for key, label, *_ in LABELS:
    print(f"  {b[key]:>4}  {label}")
if b["other"]:
    print(f"  {b['other']:>4}  Pending verification (data gap, not a result)")
print(f"confirmed winners: {len(won_uids)}")
print("office levels (race not in DDHQ):", dict(lvl))
print(
    "wrote: ddhq_unmatched_summary.csv, ddhq_unmatched_by_office_level.csv, "
    "ddhq_unmatched_detail.csv, ddhq_unmatched_confirmed_wins.csv"
)
