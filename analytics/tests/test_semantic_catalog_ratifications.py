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
        detail_doc=None,
        retired=kw.get("retired"),
        yaml_file="sem_fixture.yml",
        kind="metric",
        business_rule=kw.get("business_rule", "counts: a user"),
        anchored_on=kw.get("anchored_on"),
        measure=kw.get("measure", "user_count"),
    )


def _rule(approved="2026-08-05", sha=None, rec=None):
    return ratifications.SignOff(approved, sha or ratifications.rule_sha(rec or _rec()))


def _data(approved="2026-08-05", sha=None, rec=None, value=10):
    return ratifications.SignOff(approved, sha or ratifications.build_sha(rec or _rec()), value)


TWO_HALVES = (
    "m:\n"
    "  business:\n    approved: 2026-08-05\n    rule_sha: 'abc1234'\n"
    "    approved_by_pr: 708\n"
    "  data:\n    approved: 2026-09-01\n    build_sha: 'def5678'\n"
    "    value_at_signing: 941\n    approved_by_pr: 760\n"
)


def _sidecar(tmp_path, body):
    path = tmp_path / "ratifications.yml"
    path.write_text(body)
    return path


def test_missing_file_loads_as_nothing_ratified(tmp_path):
    # The before side of every diff parses a base commit that may predate the
    # sidecar entirely; absence has to be legal, not an error.
    assert ratifications.load(tmp_path / "absent.yml") == {}


def test_load_reads_both_halves(tmp_path):
    got = ratifications.load(_sidecar(tmp_path, TWO_HALVES))["m"]
    assert got.rule.approved == "2026-08-05" and got.rule.sha == "abc1234"
    assert got.data.approved == "2026-09-01" and got.data.sha == "def5678"
    assert got.data.value == 941
    # Per half, because the two were approved on different PRs — which is the
    # point of splitting them, and which one entry-level field cannot hold.
    assert got.rule.pr == 708 and got.data.pr == 760


def test_load_reads_one_half_and_leaves_the_other_pending(tmp_path):
    # A data-only change approved by the data group records only that half. The
    # business half stays absent, which is how the catalog says "not ruled on".
    body = "m:\n  data:\n    approved: 2026-09-01\n    build_sha: 'def5678'\n    value_at_signing: 941\n"
    got = ratifications.load(_sidecar(tmp_path, body))["m"]
    assert got.rule is None and got.data is not None


def test_load_rejects_a_half_without_its_seal(tmp_path):
    # A date nothing can check is the exact state the sidecar exists to kill.
    path = _sidecar(tmp_path, "m:\n  business:\n    approved: 2026-08-05\n")
    with pytest.raises(ValueError, match="rule_sha"):
        ratifications.load(path)


def test_load_rejects_a_data_half_without_a_value(tmp_path):
    # The requirement the ticket settled: you cannot record that a build was
    # signed off without saying what it counted that day. Enforced in the loader
    # behind the blocking freshness gate, never as a PR check, because CI cannot
    # compute a metric's value and could only check that SOME number is present.
    path = _sidecar(tmp_path, "m:\n  data:\n    approved: 2026-09-01\n    build_sha: 'def5678'\n")
    with pytest.raises(ValueError, match="value_at_signing"):
        ratifications.load(path)


def test_load_rejects_a_value_on_the_business_half(tmp_path):
    # A business approval signs the rule and carries no number; a value there
    # would assert that ruling on the words involved looking at a count.
    body = (
        "m:\n  business:\n    approved: 2026-08-05\n    rule_sha: 'abc1234'\n" "    value_at_signing: 941\n"
    )
    with pytest.raises(ValueError, match="value_at_signing"):
        ratifications.load(_sidecar(tmp_path, body))


def test_load_rejects_a_non_integer_value(tmp_path):
    body = (
        "m:\n  data:\n    approved: 2026-09-01\n    build_sha: 'def5678'\n"
        "    value_at_signing: about a thousand\n"
    )
    with pytest.raises(ValueError, match="whole number"):
        ratifications.load(_sidecar(tmp_path, body))


def test_load_rejects_an_entry_with_neither_half(tmp_path):
    with pytest.raises(ValueError, match="unknown key"):
        ratifications.load(_sidecar(tmp_path, "m:\n  approved_by_pr: 760\n"))


def test_load_rejects_an_unknown_key(tmp_path):
    body = "m:\n  business:\n    approved: 2026-08-05\n    rule_sha: 'abc1234'\n" "  ratified_by: someone\n"
    with pytest.raises(ValueError, match="unknown key"):
        ratifications.load(_sidecar(tmp_path, body))


def test_load_rejects_non_mapping_entry(tmp_path):
    with pytest.raises(ValueError, match="mapping"):
        ratifications.load(_sidecar(tmp_path, "m: 2026-08-05\n"))


def test_load_rejects_an_unquoted_all_digit_hash(tmp_path):
    # YAML reads 0123456 as an integer and the leading zero is unrecoverable,
    # so the comparison would silently report a healthy metric as stale.
    body = "m:\n  business:\n    approved: 2026-08-05\n    rule_sha: 0123456\n"
    with pytest.raises(ValueError, match="must be quoted"):
        ratifications.load(_sidecar(tmp_path, body))


def test_load_rejects_a_malformed_hash(tmp_path):
    body = "m:\n  business:\n    approved: 2026-08-05\n    rule_sha: 'NOTAHEX'\n"
    with pytest.raises(ValueError, match="lowercase hex"):
        ratifications.load(_sidecar(tmp_path, body))


def test_load_reads_a_legacy_single_seal_entry_onto_both_halves(tmp_path):
    # Only the BEFORE side of a diff against old history reaches this. Both
    # halves read stale, which is accurate: nothing under the new scheme has
    # been signed. The `legacy` marker is what lets the merge summary say
    # "re-sealed" instead of reporting five metrics as newly broken.
    body = "m:\n  ratified: 2026-08-05\n  definition_sha: 'abc1234'\n  approved_by_pr: 760\n"
    got = ratifications.load(_sidecar(tmp_path, body))["m"]
    assert got.scheme == "legacy"
    assert got.rule.approved == got.data.approved == "2026-08-05"
    assert got.data.value is None


def test_load_rejects_a_half_and_half_entry(tmp_path):
    # Half old shape, half new, is a botched hand-edit rather than history.
    body = "m:\n  ratified: 2026-08-05\n  business:\n    approved: 2026-08-05\n    rule_sha: 'abc1234'\n"
    with pytest.raises(ValueError, match="mixes"):
        ratifications.load(_sidecar(tmp_path, body))


def test_the_two_seals_move_independently():
    # The whole point. Rewording the prose used to expire an approval, and
    # changing which events count used to expire nothing. Now the rule seal
    # answers only to the rule and the build seal only to the build.
    base = _rec()
    reworded = _rec(definition="entirely different prose")
    assert ratifications.rule_sha(base) == ratifications.rule_sha(reworded)
    assert ratifications.build_sha(base) == ratifications.build_sha(reworded)

    reruled = _rec(business_rule="counts: somebody else")
    assert ratifications.rule_sha(base) != ratifications.rule_sha(reruled)
    assert ratifications.build_sha(base) == ratifications.build_sha(reruled)

    reanchored = _rec(anchored_on="event=Something Else")
    assert ratifications.rule_sha(base) == ratifications.rule_sha(reanchored)
    assert ratifications.build_sha(base) != ratifications.build_sha(reanchored)


def test_build_seal_covers_filter_measure_type_and_source():
    base = _rec()
    for kw in (
        {"filter": "{{ Dimension('x') }}"},
        {"measure": "other_count"},
        {"metric_type": "ratio"},
        {"source": "ref('other')"},
    ):
        assert ratifications.build_sha(base) != ratifications.build_sha(_rec(**kw)), kw


def test_neither_seal_covers_dimensions_or_label():
    # dimensions is a FILE-level union, so a dimension added to the semantic
    # model must not un-ratify every metric in that file. label is display text.
    base = _rec()
    other = _rec(dimensions=("registered_at", "is_activated"), label="Renamed Label")
    assert ratifications.rule_sha(base) == ratifications.rule_sha(other)
    assert ratifications.build_sha(base) == ratifications.build_sha(other)


def test_apply_sets_each_half_and_leaves_a_matching_seal_fresh():
    rec = _rec()
    applied = ratifications.apply(
        [rec], {"m": ratifications.Ratification(rule=_rule(rec=rec), data=_data(rec=rec))}
    )[0]
    assert applied.rule_approved == "2026-08-05" and applied.rule_stale is False
    assert applied.build_approved == "2026-08-05" and applied.build_stale is False
    assert applied.value_at_signing == 10


def test_apply_flags_only_the_half_that_moved():
    # A build edit must not expire the business group's ruling on the words, and
    # a reworded rule must not expire the build. Under one seal both went at once.
    signed = _rec()
    edited = _rec(anchored_on="event=Replaced")
    applied = ratifications.apply(
        [edited], {"m": ratifications.Ratification(rule=_rule(rec=signed), data=_data(rec=signed))}
    )[0]
    assert applied.rule_stale is False
    assert applied.build_stale is True


def test_apply_leaves_unlisted_metric_pending():
    applied = ratifications.apply([_rec()], {})[0]
    assert applied.rule_approved is None and applied.build_approved is None
    assert applied.rule_stale is False and applied.build_stale is False


def test_orphaned_keys_reports_sidecar_entry_with_no_metric():
    sign_offs = {"gone": ratifications.Ratification(rule=_rule())}
    assert ratifications.orphaned_keys([_rec(name="m")], sign_offs) == ["gone"]


def test_ratified_cell_renders_each_half_separately():
    assert ratified_cell(_rec()) == "pending"

    rec = _rec()
    both = ratifications.apply(
        [rec], {"m": ratifications.Ratification(rule=_rule(rec=rec), data=_data("2026-09-01", rec=rec))}
    )[0]
    assert ratified_cell(both) == "rule 2026-08-05 · build 2026-09-01"

    half = ratifications.apply([rec], {"m": ratifications.Ratification(data=_data(rec=rec))})[0]
    assert ratified_cell(half) == "rule pending · build 2026-08-05"

    stale = ratifications.apply(
        [rec],
        {
            "m": ratifications.Ratification(
                rule=ratifications.SignOff("2026-08-05", "0000000"), data=_data(rec=rec)
            )
        },
    )[0]
    assert ratified_cell(stale) == "rule 2026-08-05 (stale) · build 2026-08-05"

    # A pending metric is never marked stale: there is no claim to be stale.
    assert ratified_cell(_rec(retired="2026-08-01")) == "pending (retired 2026-08-01)"
