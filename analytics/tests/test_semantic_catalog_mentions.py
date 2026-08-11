from pathlib import Path

from semantic_catalog import mentions


def test_render_team_usergroup_takes_precedence():
    cfg = {"usergroup_id": "S0ABC", "member_ids": ["U1", "U2"]}
    assert mentions.render_team(cfg) == "<!subteam^S0ABC>"


def test_render_team_member_ids():
    assert mentions.render_team({"usergroup_id": None, "member_ids": ["U1", "U2"]}) == "<@U1> <@U2>"


def test_render_team_empty_config_degrades_to_no_mention():
    assert mentions.render_team({"usergroup_id": None, "member_ids": []}) == ""
    assert mentions.render_team(None) == ""


def test_load_missing_file_returns_empty(tmp_path):
    assert mentions.load(tmp_path / "nope.yml") == {}


def test_load_shipped_config_parses():
    shipped = Path(mentions.__file__).parent / "config" / "slack_mentions.yml"
    teams = mentions.load(shipped)
    assert set(teams) == {"data", "business"}
