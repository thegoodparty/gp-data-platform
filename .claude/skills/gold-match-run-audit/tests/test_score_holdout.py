"""Standing tests for the holdout gate's sole pass/fail authority.

Shape mirrors l2-uniform-drift-remediator/tests: stdlib unittest, direct import
for the pure functions, subprocess for the load-boundary and gate wiring. Run:

    python3 -m unittest discover .claude/skills/gold-match-run-audit/tests
"""

from __future__ import annotations

import csv
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SKILL_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SKILL_ROOT))

import score_holdout  # noqa: E402

PACKET_FIELDS = [
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
]


def packet_row(bid, stratum, cell, verdict, truth=("", "", ""), jan=("", "", "")):
    return dict(
        zip(
            PACKET_FIELDS,
            [str(bid), stratum, cell, verdict, *truth, *jan],
        )
    )


def write_packet(rows, dirpath, drop_columns=()):
    fields = [f for f in PACKET_FIELDS if f not in drop_columns]
    path = Path(dirpath) / "packet.csv"
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return path


def full_packet(poison=None, n_backlog=72, n_served=48):
    """The ratified 72/48 shape with poisonable lead rows; filler rows are valid NVDs."""
    rows = [
        packet_row(1, "backlog_C_dead_label", "school", "DISTRICT", truth=("KY", "City", "A")),
        packet_row(2, "served_matched", "city", "NO_VALID_DISTRICT"),
        packet_row(
            3,
            "served_matched",
            "subthreshold",
            "DISTRICT",
            truth=("OH", "City", "B"),
            jan=("OH", "City", "B"),
        ),
    ]
    rows += [
        packet_row(100 + i, "backlog_B_stale_abstain", "county", "NO_VALID_DISTRICT")
        for i in range(n_backlog - 1)
    ]
    rows += [
        packet_row(500 + i, "served_matched", "county", "NO_VALID_DISTRICT") for i in range(n_served - 2)
    ]
    for row in poison or []:
        rows[row.pop("_idx")].update(row)
    return rows


def full_answers(rows, matched=()):
    out = []
    for row in rows:
        bid = int(row["br_database_id"])
        if bid in matched:
            out.append(
                {
                    "br_database_id": bid,
                    "l2_state": row["truth_l2_state"],
                    "l2_district_type": row["truth_l2_district_type"],
                    "l2_district_name": row["truth_l2_district_name"],
                }
            )
        else:
            out.append(
                {"br_database_id": bid, "l2_state": None, "l2_district_type": None, "l2_district_name": None}
            )
    return out


def run_scorer(truth_path, answers=None, tmp=None):
    argv = [sys.executable, str(SKILL_ROOT / "score_holdout.py"), "--truth", str(truth_path), "--label", "t"]
    if answers is not None:
        answers_path = Path(tmp) / "answers.json"
        answers_path.write_text(json.dumps(answers))
        argv += ["--answers", str(answers_path)]
    return subprocess.run(argv, capture_output=True, text=True)


class ScoreOfficeMatrix(unittest.TestCase):
    """The 2/1/0 rule: a scoring flip here silently decides the ratified gates."""

    def test_correct_abstain_on_no_valid_district_scores_two(self):
        self.assertEqual(score_holdout.score_office("NO_VALID_DISTRICT", (None,) * 3, (None,) * 3), 2)

    def test_fabricated_match_on_no_valid_district_scores_zero(self):
        self.assertEqual(score_holdout.score_office("NO_VALID_DISTRICT", (None,) * 3, ("KY", "City", "X")), 0)

    def test_abstain_miss_on_district_scores_one(self):
        self.assertEqual(score_holdout.score_office("DISTRICT", ("KY", "City", "X"), (None,) * 3), 1)

    def test_exact_match_scores_two_and_wrong_tuple_scores_zero(self):
        truth = ("KY", "City", "SALEM-CARRSVILLE CITY")
        self.assertEqual(score_holdout.score_office("DISTRICT", truth, truth), 2)
        self.assertEqual(score_holdout.score_office("DISTRICT", truth, ("KY", "City", "OTHER")), 0)

    def test_case_and_padding_variants_still_match(self):
        # A hand-ruled truth column must not mis-score a canonical answer.
        truth = ("KY", "City", "Salem-Carrsville City ")
        self.assertEqual(
            score_holdout.score_office("DISTRICT", truth, ("KY", "City", "SALEM-CARRSVILLE CITY")), 2
        )

    def test_empty_string_answer_is_an_abstain_not_a_wrong_match(self):
        self.assertEqual(score_holdout.score_office("NO_VALID_DISTRICT", (None,) * 3, ("", "", "")), 2)


class ReportGroupRouting(unittest.TestCase):
    """Bucket routing: a mis-routed row leaves the printed tables disagreeing with the gates."""

    def test_backlog_strata_route_by_prefix(self):
        self.assertEqual(score_holdout.report_group("backlog_A_never_attempted", "city"), "backlog_A")
        self.assertEqual(score_holdout.report_group("backlog_B_stale_abstain", "school"), "backlog_B")
        self.assertEqual(score_holdout.report_group("backlog_C_dead_label", "school"), "backlog_C")

    def test_served_splits_subthreshold_from_family_cells(self):
        self.assertEqual(score_holdout.report_group("served_matched", "subthreshold"), "served subthreshold")
        self.assertEqual(score_holdout.report_group("served_matched", "city"), "served family cells")


class GateArithmetic(unittest.TestCase):
    """The two ratified gates. Strictness and the ceiling boundary are the contract."""

    def test_backlog_gate_requires_strict_superiority(self):
        challenger = {"backlog_A": [2, 2], "backlog_B": [], "backlog_C": []}
        january = {"backlog_A": [2, 2], "backlog_B": [], "backlog_C": []}
        passed, c, j = score_holdout.backlog_gate(challenger, january)
        self.assertFalse(passed)  # equal is NOT better
        challenger["backlog_A"].append(2)
        self.assertTrue(score_holdout.backlog_gate(challenger, january)[0])

    def test_served_gate_net_two_passes_and_net_three_fails(self):
        jan = {1: 2, 2: 2, 3: 2, 4: 0}
        challenger_net2 = {1: 1, 2: 1, 3: 2, 4: 0}
        self.assertTrue(score_holdout.served_gate(challenger_net2, jan, [1, 2, 3, 4])[0])
        challenger_net3 = {1: 1, 2: 1, 3: 1, 4: 0}
        self.assertFalse(score_holdout.served_gate(challenger_net3, jan, [1, 2, 3, 4])[0])

    def test_improvements_offset_regressions(self):
        jan = {1: 2, 2: 2, 3: 2, 4: 0, 5: 0, 6: 0}
        challenger = {1: 1, 2: 1, 3: 1, 4: 2, 5: 2, 6: 2}
        passed, net, reg, imp = score_holdout.served_gate(challenger, jan, [1, 2, 3, 4, 5, 6])
        self.assertEqual((reg, imp, net), (3, 3, 0))
        self.assertTrue(passed)


class LoadBoundary(unittest.TestCase):
    """A malformed operator artifact must fail loudly at load, never reroute rows."""

    def base_rows(self):
        return full_packet()

    def test_missing_cell_column_raises_at_load(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = write_packet(self.base_rows(), tmp, drop_columns=("cell",))
            result = run_scorer(path)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("missing required column", result.stderr)

    def test_duplicate_office_raises(self):
        rows = self.base_rows()
        with tempfile.TemporaryDirectory() as tmp:
            path = write_packet([rows[0]] + rows, tmp)
            result = run_scorer(path)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("duplicate br_database_id", result.stderr)

    def test_unrecognized_stratum_and_verdict_raise(self):
        for field, value, marker in (
            ("stratum", "Backlog_C", "stratum"),
            ("truth_verdict", "NO_VALID", "verdict"),
        ):
            rows = self.base_rows()
            rows[0][field] = value
            with tempfile.TemporaryDirectory() as tmp:
                result = run_scorer(write_packet(rows, tmp))
            self.assertNotEqual(result.returncode, 0, field)
            self.assertIn(marker, result.stderr)

    def test_trailing_space_verdict_is_normalized_not_rerouted(self):
        rows = self.base_rows()
        rows[1]["truth_verdict"] = "NO_VALID_DISTRICT "
        with tempfile.TemporaryDirectory() as tmp:
            result = run_scorer(write_packet(rows, tmp))
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_truncated_packet_refused(self):
        # A dropped row can delete a served regression from the gate arithmetic.
        rows = self.base_rows()[:-1]
        with tempfile.TemporaryDirectory() as tmp:
            result = run_scorer(write_packet(rows, tmp))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("packet shape", result.stderr)

    def test_partial_district_truth_tuple_refused(self):
        rows = self.base_rows()
        rows[0]["truth_l2_district_name"] = ""
        with tempfile.TemporaryDirectory() as tmp:
            result = run_scorer(write_packet(rows, tmp))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("partial truth tuple", result.stderr)


class GateWiring(unittest.TestCase):
    """main()-level guards: the wrong artifact must be refused, not scored."""

    def test_incomplete_answers_refused_not_scored_as_abstention(self):
        rows = full_packet()
        answers = full_answers(rows)[:-1]
        with tempfile.TemporaryDirectory() as tmp:
            result = run_scorer(write_packet(rows, tmp), answers=answers, tmp=tmp)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("missing 1 scorable office", result.stderr)

    def test_duplicate_answer_ids_refused_not_last_write_wins(self):
        rows = full_packet()
        answers = full_answers(rows)
        conflict = dict(answers[0])
        conflict.update(l2_state="ZZ", l2_district_type="City", l2_district_name="CONFLICT")
        with tempfile.TemporaryDirectory() as tmp:
            result = run_scorer(write_packet(rows, tmp), answers=answers + [conflict], tmp=tmp)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("duplicate br_database_id entries in the answers", result.stderr)

    def test_all_backlog_packet_refused(self):
        # The shape guard is the fail-open kill for a served-empty packet; the
        # served-pool raise in main remains as depth behind it.
        rows = full_packet(n_backlog=121, n_served=0)
        rows = [r for r in rows if r["stratum"] != "served_matched"]
        with tempfile.TemporaryDirectory() as tmp:
            result = run_scorer(write_packet(rows, tmp), answers=full_answers(rows), tmp=tmp)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("packet shape", result.stderr)

    def test_complete_run_prints_verdict(self):
        rows = full_packet()
        answers = full_answers(rows, matched=(1, 3))
        with tempfile.TemporaryDirectory() as tmp:
            result = run_scorer(write_packet(rows, tmp), answers=answers, tmp=tmp)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Backlog gate", result.stdout)
        self.assertIn("Verdict:", result.stdout)


if __name__ == "__main__":
    unittest.main()
