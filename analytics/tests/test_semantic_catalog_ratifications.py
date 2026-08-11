import pytest
from semantic_catalog import ratifications
from semantic_catalog.records import MetricRecord, ratified_cell


def _rec(name="m", definition="def", filter=None, source="ref('t')", metric_type="simple", **kw):
    return MetricRecord(
        name=name,
        label=kw.get("label", name),
        definition=definition,
        metric_type=metric_type,
        source=source,
        dimensions=kw.get("dimensions", ()),
        filter=filter,
        owner=None,
        ratified=None,
        detail_doc=None,
        retired=kw.get("retired"),
        yaml_file="sem_fixture.yml",
        kind="metric",
    )


def _sidecar(tmp_path, body):
    path = tmp_path / "ratifications.yml"
    path.write_text(body)
    return path


def test_missing_file_loads_as_nothing_ratified(tmp_path):
    # The before side of every diff parses a base commit that may predate the
    # sidecar entirely; absence has to be legal, not an error.
    assert ratifications.load(tmp_path / "absent.yml") == {}


def test_load_reads_all_three_fields(tmp_path):
    path = _sidecar(
        tmp_path, "m:\n  ratified: 2026-08-05\n  definition_sha: 'abc1234'\n  approved_by_pr: 760\n"
    )
    got = ratifications.load(path)["m"]
    assert got.ratified == "2026-08-05"
    assert got.definition_sha == "abc1234"
    assert got.approved_by_pr == 760


def test_load_rejects_entry_without_definition_sha(tmp_path):
    # A date nothing can check is the exact state the sidecar exists to kill.
    path = _sidecar(tmp_path, "m:\n  ratified: 2026-08-05\n")
    with pytest.raises(ValueError, match="definition_sha"):
        ratifications.load(path)


def test_load_rejects_non_mapping_entry(tmp_path):
    path = _sidecar(tmp_path, "m: 2026-08-05\n")
    with pytest.raises(ValueError, match="mapping"):
        ratifications.load(path)


def test_load_rejects_an_unquoted_all_digit_hash(tmp_path):
    # YAML reads 0123456 as an integer and the leading zero is unrecoverable,
    # so the comparison would silently report a healthy metric as stale.
    path = _sidecar(tmp_path, "m:\n  ratified: 2026-08-05\n  definition_sha: 0123456\n")
    with pytest.raises(ValueError, match="must be quoted"):
        ratifications.load(path)


def test_load_rejects_a_malformed_hash(tmp_path):
    path = _sidecar(tmp_path, "m:\n  ratified: 2026-08-05\n  definition_sha: 'NOTAHEX'\n")
    with pytest.raises(ValueError, match="lowercase hex"):
        ratifications.load(path)


def test_fingerprint_changes_when_definition_changes():
    assert ratifications.definition_sha(_rec(definition="a")) != ratifications.definition_sha(
        _rec(definition="b")
    )


def test_fingerprint_changes_when_filter_changes():
    assert ratifications.definition_sha(_rec(filter=None)) != ratifications.definition_sha(
        _rec(filter="{{ Dimension('user__is_activated') }}")
    )


def test_fingerprint_ignores_dimensions_and_label():
    # dimensions is a FILE-level union, so a dimension added to the semantic
    # model must not un-ratify every metric in that file. label is display text.
    base = _rec()
    assert ratifications.definition_sha(base) == ratifications.definition_sha(
        _rec(dimensions=("registered_at", "is_activated"), label="Renamed Label")
    )


def test_fingerprint_ignores_yaml_rewrapping():
    # The parser whitespace-collapses definitions, so re-wrapping a block scalar
    # must not read as a definition change.
    assert ratifications.definition_sha(_rec(definition="one two three")) == ratifications.definition_sha(
        _rec(definition="one two three")
    )


def test_apply_sets_date_and_leaves_matching_fingerprint_fresh():
    rec = _rec()
    sign_offs = {"m": ratifications.Ratification("2026-08-05", ratifications.definition_sha(rec))}
    applied = ratifications.apply([rec], sign_offs)[0]
    assert applied.ratified == "2026-08-05"
    assert applied.ratified_stale is False


def test_apply_flags_stale_when_definition_moved_since_signoff():
    signed = _rec(definition="the definition that was approved")
    sign_offs = {"m": ratifications.Ratification("2026-08-05", ratifications.definition_sha(signed))}
    edited = _rec(definition="a definition nobody approved")
    applied = ratifications.apply([edited], sign_offs)[0]
    assert applied.ratified == "2026-08-05"
    assert applied.ratified_stale is True


def test_apply_leaves_unlisted_metric_pending():
    applied = ratifications.apply([_rec()], {})[0]
    assert applied.ratified is None
    assert applied.ratified_stale is False


def test_orphaned_keys_reports_sidecar_entry_with_no_metric():
    sign_offs = {"gone": ratifications.Ratification("2026-08-05", "abc1234")}
    assert ratifications.orphaned_keys([_rec(name="m")], sign_offs) == ["gone"]


def test_ratified_cell_renders_pending_dated_stale_and_retired():
    assert ratified_cell(_rec()) == "pending"

    dated = ratifications.apply(
        [_rec()], {"m": ratifications.Ratification("2026-08-05", ratifications.definition_sha(_rec()))}
    )[0]
    assert ratified_cell(dated) == "2026-08-05"

    stale = ratifications.apply([_rec()], {"m": ratifications.Ratification("2026-08-05", "0000000")})[0]
    assert ratified_cell(stale) == "2026-08-05 (stale: definition changed since ratification)"

    # A pending metric is never marked stale: there is no claim to be stale.
    assert ratified_cell(_rec(retired="2026-08-01")) == "pending (retired 2026-08-01)"
