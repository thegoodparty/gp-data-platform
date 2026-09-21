#!/usr/bin/env python3
"""Row-existence first pass for the supervised-monitoring audit (SKILL.md, "Supervised monitoring of a matcher rule").

For each in-class abstain in a shadow CSV, ask the rule's own question against EVERY row in the state's universe, of
any type: does a row carry the body's anchor tokens? Zero hits means no correct row can exist under standard 1 unless
an abbreviation hid it (sample-check those); hits go to hand review with the best-scoring rows listed. Run from
gold-match so body_presence imports:

    uv run python find_body_rows.py shadow-<date>.csv universe-<date>.csv hits-<date>.csv
"""

from __future__ import annotations

import csv
import re
import sys
from collections import defaultdict

from build_shadow_rows import ABSTAIN_LABELS
from stitch_golden_data.prod_gold_data import body_presence as bp


def _row_tokens(name):
    return {bp.PROPER_ABBREVIATIONS.get(x, x) for x in bp.tokens(re.sub(r"\([^)]*\)", " ", name))}


def body_rows(shadow, universe):
    by_state = defaultdict(list)
    for state, dtype, dname in universe:
        by_state[state.strip().upper()].append((dtype, dname, _row_tokens(dname)))
    out = []
    for s in shadow:
        if s["rule_class"] not in ABSTAIN_LABELS:
            continue
        rows = by_state[s["state"].strip().upper()]
        generic = bp.GENERIC_WORDS | bp.type_words([t for t, _, _ in rows])
        anchors = bp.anchor_tokens(s["name"], generic)
        body = {bp.PROPER_ABBREVIATIONS.get(t, t) for t in bp.tokens(bp._body(s["name"]))} - bp.ROLE_WORDS
        hits = [n for _, n, toks in rows if anchors and all(bp._carries(toks, a) for a in anchors)]
        # Dice over exact tokens ranks the body's own row above a longer lookalike that merely contains it.
        scored = sorted(
            ((2 * len(body & toks) / (len(body) + len(toks)), t, n) for t, n, toks in rows if body & toks),
            reverse=True,
        )[:3]
        out.append(
            {
                "br_database_id": s["br_database_id"],
                "name": s["name"],
                "state": s["state"],
                "rule_class": s["rule_class"],
                "anchors": " ".join(sorted(anchors)),
                "anchor_hits": len(hits),
                "top_candidates": "; ".join(f"{t}: {n} ({score:.2f})" for score, t, n in scored),
            }
        )
    return out


def main(argv=None):
    shadow_path, universe_path, out_path = (argv or sys.argv[1:])[:3]
    shadow = list(csv.DictReader(open(shadow_path, newline="")))
    universe = [
        (u["state_postal_code"], u["district_type"], u["district_name"])
        for u in csv.DictReader(open(universe_path, newline="", encoding="utf-8-sig"))
    ]
    rows = body_rows(shadow, universe)
    with open(out_path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()) if rows else ["br_database_id"])
        w.writeheader()
        w.writerows(rows)
    with_hits = sum(r["anchor_hits"] > 0 for r in rows)
    print(
        f"{len(rows)} in-class abstains: {len(rows) - with_hits} with no anchor hit, {with_hits} with hits -> {out_path}"
    )


if __name__ == "__main__":
    main()
