from semantic_catalog import naming_guard


def test_real_repo_has_no_misplaced_semantic_files():
    # Every governed metric today lives in a sem_*.yml; the guard must pass clean
    # on the real tree, or CI would be red on an untouched repo.
    assert naming_guard.find_misplaced() == []


def _write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def test_flags_semantic_block_in_non_sem_file(tmp_path):
    # A metrics: block in a differently-named file bypasses the path filter.
    _write(tmp_path / "marts" / "schema.yml", "metrics:\n  - name: sneaky\n")
    assert naming_guard.find_misplaced(tmp_path) == [tmp_path / "marts" / "schema.yml"]


def test_flags_wrong_extension(tmp_path):
    # sem_ prefix but .yaml: the *.yml path filter and the parser both miss it.
    _write(tmp_path / "sem_thing.yaml", "semantic_models:\n  - name: m\n")
    assert naming_guard.find_misplaced(tmp_path) == [tmp_path / "sem_thing.yaml"]


def test_ignores_governed_sem_file(tmp_path):
    _write(tmp_path / "marts" / "sem_users.yml", "metrics:\n  - name: ok\n")
    assert naming_guard.find_misplaced(tmp_path) == []


def test_ignores_ordinary_yaml_without_semantics(tmp_path):
    # A normal dbt schema/model-properties file (no semantic block) is fine.
    _write(tmp_path / "marts" / "schema.yml", "models:\n  - name: some_model\n")
    assert naming_guard.find_misplaced(tmp_path) == []


def test_malformed_yaml_does_not_crash(tmp_path):
    # check-yaml owns syntax errors; the guard must not raise on them.
    _write(tmp_path / "broken.yml", "metrics: [unclosed\n")
    assert naming_guard.find_misplaced(tmp_path) == []


def test_main_returns_zero_on_clean_repo():
    assert naming_guard.main() == 0
