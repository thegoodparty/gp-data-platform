#!/usr/bin/env python3
"""Score a matcher run (or January itself) against the ratified gold-match holdout gate.

Stdlib only: this has no repo import and no venv dependency, so it runs standalone
against a run's answers JSON and the adjudicated holdout packet CSV. See
.claude/skills/gold-match-run-audit/SKILL.md Step 4 for usage.
"""

import argparse
import csv
import json
import statistics
from collections import defaultdict

# The one number this script owns: the ratified served-gate ceiling. The backlog
# gate is strict superiority, so it carries no threshold of its own.
SERVED_NET_REGRESSION_MAX = 2

SERVED_STRATUM = "served_matched"
SUBTHRESHOLD_CELL = "subthreshold"
SERVED_FAMILY_GROUP = "served family cells"
SERVED_SUBTHRESHOLD_GROUP = "served subthreshold"
BACKLOG_GROUPS = ("backlog_A", "backlog_B", "backlog_C")
REPORT_ORDER = (*BACKLOG_GROUPS, SERVED_FAMILY_GROUP, SERVED_SUBTHRESHOLD_GROUP)


def report_group(stratum, cell):
    """The five report buckets, derived from the packet's own stratum/cell naming."""
    if stratum.startswith("backlog_"):
        return "_".join(stratum.split("_")[:2])
    return SERVED_SUBTHRESHOLD_GROUP if cell == SUBTHRESHOLD_CELL else SERVED_FAMILY_GROUP


def _norm_tuple(t):
    """Case-fold for comparison only: universe labels are unique case-insensitively,
    and a hand-ruled truth column must not mis-score a canonical challenger answer
    over casing. Empty strings mean absent (a CSV round-trip turns SQL NULL into '')."""
    return tuple(p.strip().lower() if p else None for p in t)


def score_office(truth_verdict, truth_tuple, answer_tuple):
    """2 correct / 1 abstain-miss / 0 wrong-match. A wrong match scores below an abstain.

    UNDETERMINABLE rows never reach this function; callers filter them first.
    """
    truth_tuple, answer_tuple = _norm_tuple(truth_tuple), _norm_tuple(answer_tuple)
    matched = answer_tuple[2] is not None
    if truth_verdict == "NO_VALID_DISTRICT":
        return 2 if not matched else 0
    if not matched:  # truth_verdict == "DISTRICT"
        return 1
    return 2 if answer_tuple == truth_tuple else 0


def tuple_of(row, prefix):
    """The (state, type, name) tuple from one of the packet's own column families."""
    return (
        row[f"{prefix}_l2_state"] or None,
        row[f"{prefix}_l2_district_type"] or None,
        row[f"{prefix}_l2_district_name"] or None,
    )


def challenger_tuple_of(row, answers):
    # Direct indexing on purpose (completeness is validated up front, and a
    # malformed answers file must fail loudly, not score as abstains). `or None`
    # because a CSV round-trip can turn SQL NULL into '', which must stay an
    # abstain, not a wrong match.
    answer = answers[int(row["br_database_id"])]
    return (
        answer["l2_state"] or None,
        answer["l2_district_type"] or None,
        answer["l2_district_name"] or None,
    )


VALID_VERDICTS = ("DISTRICT", "NO_VALID_DISTRICT", "UNDETERMINABLE")
VALID_STRATA = (
    "backlog_A_never_attempted",
    "backlog_B_stale_abstain",
    "backlog_C_dead_label",
    SERVED_STRATUM,
)


def load_truth(path):
    """Normalize and validate the human-edited columns at the load boundary: a
    stray-whitespace verdict, a typo'd verdict or stratum, or a duplicated office
    must fail loudly here, never silently reroute rows around the gates.
    utf-8-sig so a spreadsheet re-save's BOM cannot mangle the first fieldname.
    """
    with open(path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)
        fieldnames = set(reader.fieldnames or [])
    required = {
        "br_database_id",
        "stratum",
        "cell",
        "truth_verdict",
        "truth_l2_state",
        "truth_l2_district_type",
        "truth_l2_district_name",
        "jan_l2_state",
        "jan_l2_district_type",
        "jan_l2_district_name",
    }
    missing_cols = required - fieldnames
    if missing_cols:
        raise ValueError(f"packet CSV is missing required column(s): {sorted(missing_cols)}")
    seen = set()
    for row in rows:
        bid = row["br_database_id"]
        if bid in seen:
            raise ValueError(f"duplicate br_database_id {bid} in the packet")
        seen.add(bid)
        if row["stratum"] not in VALID_STRATA:
            raise ValueError(f"unrecognized stratum {row['stratum']!r} for office {bid}")
        row["truth_verdict"] = (row["truth_verdict"] or "").strip()
        if row["truth_verdict"] not in VALID_VERDICTS:
            raise ValueError(f"unrecognized truth_verdict {row['truth_verdict']!r} for office {bid}")
        for field in ("truth_l2_state", "truth_l2_district_type", "truth_l2_district_name"):
            row[field] = (row[field] or "").strip()
    return rows


def load_answers(path):
    if path is None:
        return None
    with open(path) as fh:
        payload = json.load(fh)
    return {int(row["br_database_id"]): row for row in payload}


def score_all(rows, answers):
    """Score every non-UNDETERMINABLE row for one arm (answers=None scores January)."""
    scores_by_bid = {}
    groups = defaultdict(list)
    for row in rows:
        if row["truth_verdict"] == "UNDETERMINABLE":
            continue
        bid = int(row["br_database_id"])
        truth = tuple_of(row, "truth")
        answer = tuple_of(row, "jan") if answers is None else challenger_tuple_of(row, answers)
        score = score_office(row["truth_verdict"], truth, answer)
        scores_by_bid[bid] = score
        groups[report_group(row["stratum"], row["cell"])].append(score)
    return scores_by_bid, groups


def print_table(label, groups):
    print(f"\n{label}")
    print(f"{'group':22s}{'n':>5s}{'correct':>9s}{'abstain-miss':>14s}{'wrong-match':>13s}{'mean':>7s}")
    for key in REPORT_ORDER:
        scores = groups.get(key, [])
        n = len(scores)
        correct = sum(1 for s in scores if s == 2)
        abstain_miss = sum(1 for s in scores if s == 1)
        wrong = sum(1 for s in scores if s == 0)
        mean = statistics.mean(scores) if scores else 0.0
        print(f"{key:22s}{n:>5d}{correct:>9d}{abstain_miss:>14d}{wrong:>13d}{mean:>7.2f}")


def backlog_gate(challenger_groups, jan_groups):
    def correct_count(groups):
        return sum(score == 2 for group in BACKLOG_GROUPS for score in groups.get(group, []))

    challenger_correct = correct_count(challenger_groups)
    jan_correct = correct_count(jan_groups)
    return challenger_correct > jan_correct, challenger_correct, jan_correct


def served_gate(challenger_scores, jan_scores, served_bids):
    regressions = sum(1 for bid in served_bids if challenger_scores[bid] < jan_scores[bid])
    improvements = sum(1 for bid in served_bids if challenger_scores[bid] > jan_scores[bid])
    net = regressions - improvements
    return net <= SERVED_NET_REGRESSION_MAX, net, regressions, improvements


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--truth", required=True, help="adjudicated holdout packet CSV")
    parser.add_argument("--answers", help="challenger answers JSON; omit to score January itself")
    parser.add_argument("--label", required=True, help="printed in the report header")
    args = parser.parse_args()

    rows = load_truth(args.truth)
    answers = load_answers(args.answers)
    if answers is not None:
        # Arm artifacts cover all 120 frozen offices by construction; a gap means
        # the wrong file (e.g. a production run's rows, which structurally lack
        # the served stratum) and would otherwise score as mass abstention.
        missing = [
            r["br_database_id"]
            for r in rows
            if r["truth_verdict"] != "UNDETERMINABLE" and int(r["br_database_id"]) not in answers
        ]
        if missing:
            raise ValueError(
                f"answers file missing {len(missing)} scorable office(s), e.g. {missing[:5]}; "
                "score complete holdout-arm artifacts, never a run's own rows"
            )

    excluded = sum(1 for row in rows if row["truth_verdict"] == "UNDETERMINABLE")
    print(f"Excluded (UNDETERMINABLE): {excluded}")

    challenger_scores, challenger_groups = score_all(rows, answers)
    print_table(args.label, challenger_groups)

    if answers is None:
        return

    jan_scores, jan_groups = score_all(rows, None)
    print_table("January (baseline), same offices", jan_groups)

    served_bids = [
        int(row["br_database_id"])
        for row in rows
        if row["truth_verdict"] != "UNDETERMINABLE" and row["stratum"] == SERVED_STRATUM
    ]
    if not served_bids:
        # An empty served pool would make the served gate pass vacuously.
        raise ValueError("no scorable served_matched rows in the packet; refusing a fail-open gate")
    backlog_pass, challenger_correct, jan_correct = backlog_gate(challenger_groups, jan_groups)
    served_pass, net, regressions, improvements = served_gate(challenger_scores, jan_scores, served_bids)

    print(
        f"\nBacklog gate (strict superiority): challenger {challenger_correct} vs "
        f"January {jan_correct} -> {'PASS' if backlog_pass else 'FAIL'}"
    )
    print(
        f"Served gate (net regressions <= {SERVED_NET_REGRESSION_MAX}): "
        f"{regressions} regressions, {improvements} improvements, net {net} -> "
        f"{'PASS' if served_pass else 'FAIL'}"
    )
    print(f"Verdict: {'PASS' if backlog_pass and served_pass else 'FAIL'}")


if __name__ == "__main__":
    main()
