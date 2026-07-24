from semantic_catalog.md_catalog import (
    BEGIN_MARK,
    END_MARK,
    render_region,
    render_rows,
    splice_region,
)
from semantic_catalog.records import MetricRecord


def _rec(**kw):
    base = dict(
        name="activated_users",
        label="Activated Candidates",
        definition="Has sent at least one voter outreach campaign.",
        metric_type="simple",
        source="ref('users_win_base')",
        dimensions=("is_activated",),
        filter=None,
        owner="semantic-layer-business",
        ratified="2026-07-24",
        detail_doc="engagement.md",
        retired=None,
        yaml_file="sem_analytics__users_win.yml",
        kind="metric",
    )
    base.update(kw)
    return MetricRecord(**base)


def test_render_rows_has_header_and_row():
    out = render_rows([_rec()])
    assert "| Concept |" in out
    assert "Activated Candidates" in out
    assert "engagement.md" in out
    assert "2026-07-24" in out


def test_pending_renders_pending():
    out = render_rows([_rec(ratified=None, owner=None)])
    assert "pending" in out


def test_render_region_wraps_in_markers():
    out = render_region([_rec()])
    assert out.startswith(BEGIN_MARK)
    assert out.rstrip().endswith(END_MARK)


def test_splice_replaces_existing_region():
    existing = f"# Title\n\nintro\n\n{BEGIN_MARK}\nOLD\n{END_MARK}\n\nfooter\n"
    region = render_region([_rec()])
    out = splice_region(existing, region)
    assert "OLD" not in out
    assert "Activated Candidates" in out
    assert out.startswith("# Title")
    assert out.rstrip().endswith("footer")


def test_splice_appends_when_no_markers():
    existing = "# Title\n\nbody\n"
    out = splice_region(existing, render_region([_rec()]))
    assert out.startswith("# Title")
    assert BEGIN_MARK in out


def test_splice_rejects_half_marked_file():
    import pytest

    with pytest.raises(ValueError):
        splice_region(f"x {BEGIN_MARK} y", render_region([_rec()]))
