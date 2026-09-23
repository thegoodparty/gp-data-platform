"""build_shadow_rows joins a run's offices (with production's outcome) to the mirror's rule classes and
renders the rows for the supervised-monitoring table. Catches: the divergence flag firing on the wrong
side, a state stamped with another state's universe version, and an INSERT the Statement API would mis-quote."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import build_shadow_rows as bsr  # noqa: E402

RUN = dict(
    run_key="2026-09-18 14:30:00",
    image_git_sha="4657a605",
    shape="B_live",
    universe_loaded_at={"OR": "2026-09-18 08:07:00", "FL": "2026-09-17 08:22:00"},
)


def office(br_database_id, l2_district_type="", l2_district_name="", confidence="", **kw):
    o = dict(
        br_database_id=br_database_id,
        name="Toledo City Council - Position 5",
        state="OR",
        mtfcc="G4110",
        geo_id="4174000",
        sub_area_name="Position",
        sub_area_value="5",
        is_judicial="false",
        has_unknown_boundaries="true",
        l2_district_type=l2_district_type,
        l2_district_name=l2_district_name,
        confidence=confidence,
    )
    o.update(kw)
    return o


def test_divergent_only_when_the_rule_abstains_and_production_served():
    offices = [
        office("1", "City", "TOLEDO", "90"),
        office("2"),
        office("3", "City", "TOLEDO", "80"),
    ]
    classes = {
        "1": "R2_slice_body_absent_abstain",
        "2": "R2_slice_body_absent_abstain",
        "3": "R2_slice_asserted",
    }
    by_id = {r["br_database_id"]: r for r in bsr.shadow_rows(offices, classes, **RUN)}
    assert by_id[1]["divergent"] is True
    assert by_id[2]["divergent"] is False
    assert by_id[3]["divergent"] is False
    assert by_id[2]["prod_l2_district_type"] is None and by_id[2]["prod_confidence"] is None
    assert by_id[1]["prod_confidence"] == 90 and by_id[1]["has_unknown_boundaries"] is True
    assert by_id[1]["rule_class"] == "R2_slice_body_absent_abstain" and by_id[1]["shape"] == "B_live"


def test_each_office_carries_its_own_state_universe_version():
    offices = [office("1"), office("4", state="FL", name="Lee County Commission - District 5")]
    classes = {"1": "R2_slice_body_absent_abstain", "4": "R2_slice_asserted"}
    by_id = {r["br_database_id"]: r for r in bsr.shadow_rows(offices, classes, **RUN)}
    assert by_id[1]["universe_loaded_at"] == "2026-09-18 08:07:00"
    assert by_id[4]["universe_loaded_at"] == "2026-09-17 08:22:00"


def test_insert_sql_quotes_the_statement_api_way():
    [row] = bsr.shadow_rows([office("7", name="O'Brien Ward 1")], {"7": "R2_slice_asserted"}, **RUN)
    sql = bsr.insert_sql("cat.sch.tbl", [row])
    assert sql.startswith("insert into cat.sch.tbl (run_key, image_git_sha, shape, br_database_id, name")
    assert "timestamp'2026-09-18 14:30:00'" in sql
    assert "'O\\'Brien Ward 1'" in sql
    assert ", 7, " in sql and ", NULL, NULL, NULL, false)" in sql


def test_insert_is_chunked_for_the_cli_argument_limit():
    ids = [str(i) for i in range(1, 1201)]
    rows = bsr.shadow_rows([office(i) for i in ids], dict.fromkeys(ids, "R2_slice_asserted"), **RUN)
    chunks = bsr.insert_sql_chunks("cat.sch.tbl", rows, size=500)
    assert [c.count("\n(") for c in chunks] == [500, 500, 200]
    assert all(c.startswith("insert into cat.sch.tbl (") for c in chunks)
