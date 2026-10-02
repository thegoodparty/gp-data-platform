# tests/test_person_clustering.py
"""The admission rules for person groups. Each test is one rule the design
rests on; the full-universe check is that this port reproduced dbt's #982
partition on all 1,137,780 records."""

from pathlib import Path

import pandas as pd
from click.testing import CliRunner

from scripts.cli import cli
from scripts.configs.person import PERSON_CONFIG
from scripts.person_clustering import cluster_people
from scripts.pipeline import filter_pairwise


def _nodes(*keys: str) -> pd.DataFrame:
    return pd.DataFrame({"record_key": keys, "source_name": [k.split("|")[0] for k in keys]})


def _links(*pairs: tuple[str, str], conflict: tuple[tuple[str, str], ...] = ()) -> pd.DataFrame:
    rows = [(min(a, b), max(a, b), False) for a, b in pairs] + [
        (min(a, b), max(a, b), True) for a, b in conflict
    ]
    return pd.DataFrame(rows, columns=["record_key_1", "record_key_2", "is_conflict"])


def _pairs(*pairs: tuple[str, str, float]) -> pd.DataFrame:
    return pd.DataFrame(pairs, columns=["unique_id_l", "unique_id_r", "match_probability"])


def _groups(out: pd.DataFrame) -> dict[str, str]:
    return dict(zip(out.record_key, out.person_group_key, strict=True))


def _reasons(out: pd.DataFrame) -> dict[str, str]:
    return dict(zip(out.record_key, out.rejected_reason, strict=True))


def test_native_links_close_transitively_and_conflict_links_do_not():
    nodes = _nodes("hubspot|1", "gp_api|1", "ballotready|9", "ddhq|5", "hubspot|2")
    links = _links(
        ("hubspot|1", "gp_api|1"), ("gp_api|1", "ballotready|9"), conflict=(("ddhq|5", "hubspot|2"),)
    )
    out = cluster_people(_pairs(), links, nodes, threshold=0.95)
    g = _groups(out)
    assert g["hubspot|1"] == g["gp_api|1"] == g["ballotready|9"] == "ballotready|9"
    assert g["ddhq|5"] == "ddhq|5" and g["hubspot|2"] == "hubspot|2"


def test_component_reaching_two_br_people_dissolves():
    nodes = _nodes("ballotready|1", "ballotready|2", "hubspot|1", "gp_api|1")
    links = _links(("hubspot|1", "ballotready|1"), ("hubspot|1", "gp_api|1"), ("gp_api|1", "ballotready|2"))
    out = cluster_people(_pairs(), links, nodes, threshold=0.95)
    assert set(out.identity_key) == set(nodes.record_key), "every member stands alone"


def test_clique_is_admitted_and_path_is_refused():
    nodes = _nodes("hubspot|1", "hubspot|2", "hubspot|3", "hubspot|4", "hubspot|5", "hubspot|6")
    clique = [
        ("hubspot|1", "hubspot|2", 0.99),
        ("hubspot|2", "hubspot|3", 0.99),
        ("hubspot|1", "hubspot|3", 0.99),
    ]
    path = [("hubspot|4", "hubspot|5", 0.99), ("hubspot|5", "hubspot|6", 0.99)]
    out = cluster_people(_pairs(*clique, *path), _links(), nodes, threshold=0.95)
    g, r = _groups(out), _reasons(out)
    assert g["hubspot|1"] == g["hubspot|2"] == g["hubspot|3"] == "hubspot|1"
    assert len({g["hubspot|4"], g["hubspot|5"], g["hubspot|6"]}) == 3
    assert r["hubspot|5"] == "incomplete_support" and pd.isna(r["hubspot|1"])


def test_two_br_people_cannot_link_even_as_a_clique():
    nodes = _nodes("ballotready|1", "ballotready|2", "hubspot|1")
    pairs = _pairs(
        ("ballotready|1", "hubspot|1", 0.999),
        ("ballotready|2", "hubspot|1", 0.999),
        ("ballotready|1", "ballotready|2", 0.999),
    )
    out = cluster_people(pairs, _links(), nodes, threshold=0.95)
    assert out.person_group_key.nunique() == 3
    assert set(out.rejected_reason) == {"br_cannot_link"}


def test_contraction_lets_one_member_vouch_for_the_identity():
    """A {BR, TS} candidacy pair is one identity, so a single TS<->HubSpot score
    is a supported pair of two. Flattening this cost 37% of the merge queue."""
    nodes = _nodes("ballotready|1", "techspeed|c__primary", "hubspot|1")
    links = _links(("ballotready|1", "techspeed|c__primary"))
    out = cluster_people(_pairs(("techspeed|c", "hubspot|1", 0.99)), links, nodes, threshold=0.95)
    assert out.person_group_key.nunique() == 1


def test_techspeed_prematch_key_fans_out_to_every_stage_record():
    nodes = _nodes("techspeed|c__primary", "techspeed|c__general", "hubspot|1")
    links = _links(("techspeed|c__primary", "techspeed|c__general"))
    out = cluster_people(_pairs(("hubspot|1", "techspeed|c", 0.99)), links, nodes, threshold=0.95)
    assert out.person_group_key.nunique() == 1


def test_threshold_and_string_typed_inputs():
    """er_source tables and Databricks reads arrive as strings."""
    nodes = _nodes("hubspot|1", "hubspot|2", "hubspot|3", "gp_api|1")
    links = pd.DataFrame(
        [("gp_api|1", "hubspot|3", "false"), ("hubspot|1", "hubspot|3", "true")],
        columns=["record_key_1", "record_key_2", "is_conflict"],
    )
    pairs = pd.DataFrame(
        [("hubspot|1", "hubspot|2", "0.951"), ("hubspot|2", "hubspot|3", "0.5")],
        columns=["unique_id_l", "unique_id_r", "match_probability"],
    )
    g = _groups(cluster_people(pairs, links, nodes, threshold=0.95))
    assert (
        g["hubspot|1"] == g["hubspot|2"]
        and g["hubspot|3"] == g["gp_api|1"]
        and g["hubspot|1"] != g["hubspot|3"]
    )


def test_scored_key_missing_from_nodes_drops_its_pairs_and_is_reported(capsys):
    """A pairwise vintage is older than the nodes it is re-clustered against, so
    a record deleted since scoring must not merge anything, and must be counted."""
    nodes = _nodes("hubspot|1", "hubspot|2")
    pairs = _pairs(("hubspot|1", "hubspot|2", 0.99), ("hubspot|2", "hubspot|gone", 0.99))
    out = cluster_people(pairs, _links(), nodes, threshold=0.95)
    assert out.person_group_key.nunique() == 1
    assert "drift): 1" in capsys.readouterr().out


def test_name_gate_drops_alias_only_pairs_without_a_contact_key():
    base = {
        "gamma_first_name": 3,
        "br_candidate_id_l": None,
        "br_candidate_id_r": None,
        "phone_l": None,
        "phone_r": None,
    }
    pairwise = pd.DataFrame(
        [
            {
                **base,
                "unique_id_l": "a",
                "unique_id_r": "b",
                "first_name_l": "antonio",
                "first_name_r": "antoinette",
                "email_l": None,
                "email_r": None,
            },
            {
                **base,
                "unique_id_l": "c",
                "unique_id_r": "d",
                "first_name_l": "antonio",
                "first_name_r": "antoinette",
                "email_l": "x@y.z",
                "email_r": "x@y.z",
            },
            {
                **base,
                "unique_id_l": "e",
                "unique_id_r": "f",
                "first_name_l": "ben",
                "first_name_r": "benjamin",
                "email_l": None,
                "email_r": None,
            },
        ]
    )
    kept = filter_pairwise(pairwise, PERSON_CONFIG)
    assert set(kept.unique_id_l) == {"c", "e"}


def test_recluster_command_writes_groups_from_csv_inputs(tmp_path: Path):
    _nodes("hubspot|1", "gp_api|1", "hubspot|2").to_csv(tmp_path / "nodes.csv", index=False)
    _links(("hubspot|1", "gp_api|1")).to_csv(tmp_path / "links.csv", index=False)
    pd.DataFrame(
        [
            {
                "unique_id_l": "gp_api|1",
                "unique_id_r": "hubspot|2",
                "match_probability": 0.99,
                "gamma_first_name": 4,
                "br_candidate_id_l": None,
                "br_candidate_id_r": None,
                "email_l": None,
                "email_r": None,
                "phone_l": None,
                "phone_r": None,
                "first_name_l": "jane",
                "first_name_r": "jane",
            }
        ]
    ).to_csv(tmp_path / "pairwise.csv", index=False)
    out_dir = tmp_path / "out"
    result = CliRunner().invoke(
        cli,
        [
            "recluster",
            "--entity-type",
            "person",
            "--pairwise",
            str(tmp_path / "pairwise.csv"),
            "--links",
            str(tmp_path / "links.csv"),
            "--nodes",
            str(tmp_path / "nodes.csv"),
            "--output-dir",
            str(out_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    groups = pd.read_csv(out_dir / PERSON_CONFIG.groups_output_name)
    assert groups.person_group_key.nunique() == 1
