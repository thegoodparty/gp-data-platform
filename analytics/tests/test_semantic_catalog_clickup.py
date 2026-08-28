from semantic_catalog.clickup_page import CATALOG_BEGIN, CATALOG_END, render_page
from semantic_catalog.lifecycle import Lifecycle
from semantic_catalog.records import MetricRecord

OWNERS = {
    "teams": {
        "data": {"slug": "semantic-layer-data", "role": "Technical review."},
        "business": {"slug": "semantic-layer-business", "role": "Ratification."},
    },
}


def _rec():
    return MetricRecord(
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


def _lifecycles():
    return {"sem_analytics__users_win.yml": Lifecycle("2026-06-01", "610", "2026-07-24", "720")}


def test_page_has_three_parts():
    page = render_page([_rec()], _lifecycles(), "## How the semantic layer is updated\nSOP.", OWNERS)
    assert "How the semantic layer is updated" in page  # static part 1
    assert "Decision-makers" in page  # static part 2
    assert CATALOG_BEGIN in page and CATALOG_END in page  # dynamic part 3


def test_footer_renders_after_catalog():
    page = render_page(
        [_rec()],
        _lifecycles(),
        "SOP",
        OWNERS,
        footer_md="## How governance works\n## What this means for you\nBROADCAST",
    )
    assert "What this means for you" in page
    # Footer must come after the catalog block, not before it.
    assert page.index(CATALOG_END) < page.index("What this means for you")


def test_footer_omitted_when_empty():
    page = render_page([_rec()], _lifecycles(), "SOP", OWNERS)
    assert "What this means for you" not in page


def test_catalog_row_has_lifecycle_and_governance():
    page = render_page([_rec()], _lifecycles(), "SOP", OWNERS)
    assert "Activated Candidates" in page
    assert "2026-06-01" in page and "#610" in page  # created + PR
    assert "2026-07-24" in page and "#720" in page  # last updated + PR
    assert "engagement.md" in page


def test_decision_makers_render_slugs_without_people():
    page = render_page([_rec()], _lifecycles(), "SOP", OWNERS, people=None)
    assert "semantic-layer-data" in page
    assert "semantic-layer-business" in page


def test_decision_makers_use_people_when_present():
    people = {"semantic-layer-data": ["Alice", "Bob"], "semantic-layer-business": ["Charlie"]}
    page = render_page([_rec()], _lifecycles(), "SOP", OWNERS, people=people)
    assert "Alice" in page and "Charlie" in page


def test_catalog_escapes_pipes_in_cells():
    """Verify that pipe characters in metric metadata are escaped to prevent table corruption."""
    rec_with_pipe = MetricRecord(
        name="test_metric",
        label="Test Metric | With Pipe",
        definition="count where is_winner | has_source",
        metric_type="simple",
        source="ref('test_table') | filter",
        dimensions=("dim1",),
        filter=None,
        owner="semantic-layer-data | backup",
        ratified="2026-07-24",
        detail_doc="test.md | backup.md",
        retired=None,
        yaml_file="test.yml",
        kind="metric",
    )
    page = render_page([rec_with_pipe], {}, "SOP", OWNERS)
    # Verify escaped pipes appear in output (raw unescaped pipes should not break table)
    assert "is_winner \\| has_source" in page
    assert "With Pipe" in page  # label should still be readable (escaped internally)
    # Ensure catalog markers are present (table not corrupted)
    assert CATALOG_BEGIN in page and CATALOG_END in page
