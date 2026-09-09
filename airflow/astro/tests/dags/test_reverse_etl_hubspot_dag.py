"""Structure assertions for the reverse-ETL HubSpot contact-sync daily DAG.

Loaded from the file path directly rather than the configured dags_folder, matching
test_gold_match_daily_dag.py: CI does not point dags_folder at astro/dags, and building
the DagBag at collection time keeps this on real Airflow with no metastore dependency.
"""

import logging
import re
import sys
import threading
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from airflow.models import DagBag
from pendulum import duration


@contextmanager
def suppress_logging(namespace):
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


_DAG_FILE = str(Path(__file__).resolve().parents[2] / "dags" / "reverse_etl_hubspot.py")
with suppress_logging("airflow"):
    _DAG = DagBag(dag_folder=_DAG_FILE).dags.get("reverse_etl_hubspot")

# reverse-etl/.env.example, reached from this file's path (worktree root is four levels
# up: dags -> tests -> astro -> airflow -> root).
_ENV_EXAMPLE = Path(__file__).resolve().parents[4] / "reverse-etl" / ".env.example"
_ENV_DECLARATION = re.compile(r"^\s*#?\s*([A-Z][A-Z0-9_]*)=")


def _declared_env_vars() -> set[str]:
    """Every `KEY=` retl's own .env.example declares, commented-out lines included: a
    `# DATABRICKS_CLIENT_ID=` line still names a variable retl reads."""
    lines = _ENV_EXAMPLE.read_text().splitlines()
    matches = (_ENV_DECLARATION.match(line) for line in lines)
    return {m.group(1) for m in matches if m}


def test_dag_loads():
    assert _DAG is not None, f"reverse_etl_hubspot failed to load from {_DAG_FILE}"


def test_schedule_contract_and_paused_on_creation():
    """17:00 UTC sits after the 12:02 build's worst case and gold-match's rebuild
    window, and before the 00:00 sync — the cron must not drift. Paused on creation:
    BUILD must not schedule anything; unpausing is the owner-gated enable step."""
    assert _DAG.timetable.expression == "0 17 * * *"
    assert _DAG.is_paused_upon_creation is True
    assert _DAG.catchup is False


def test_one_writer_against_the_log_table_at_a_time():
    """A manual trigger during a scheduled run must queue, not start a second writer
    against the flow's send-log table."""
    assert _DAG.max_active_runs == 1


def test_retries_meet_the_repo_floor():
    """test_dag_example.py::test_dag_retries asserts >= 2 for every DAG."""
    assert _DAG.default_args["retries"] >= 2


def test_execution_timeout_is_finite_and_bounds_a_hung_pod():
    assert _DAG.get_task("send_pod").execution_timeout == duration(hours=2)


def test_daily_invocation_carries_no_ceremony_flags():
    """A flagged daily task permanently disarms the empty-log amnesia guard: --init-log
    and --accept-empty-log are ceremony-only (first convergence, sales preview,
    supervised post-reset runs)."""
    args = _DAG.get_task("send_pod").arguments
    assert "--init-log" not in args
    assert "--accept-empty-log" not in args


def test_arguments_are_exactly_the_flow_and_destination_the_spec_names():
    assert _DAG.get_task("send_pod").arguments == [
        "--source=hubspot",
        "--destination=hubspot_contacts",
    ]


def test_pod_image_requires_the_tag_variable_with_no_mutable_default():
    """An unattended loop with a `latest` default silently runs whatever main last
    published after every merge — the evaluated artifact must be the production
    artifact, so the Variable (the merged build's sha) is required and an unset value
    fails at render, before any pod runs."""
    pod = _DAG.get_task("send_pod")
    assert "var.value.reverse_etl_image_tag" in pod.image
    assert "latest" not in pod.image
    assert pod.image.startswith("ghcr.io/thegoodparty/gp-data-platform/reverse-etl:")
    assert pod.image_pull_policy == "Always"


def test_pod_declares_no_credentials_before_it_runs():
    """Airflow snapshots rendered template fields (and the KPO pod YAML) into the
    metadata DB BEFORE pre_execute; env resolved in pre_execute never reaches that
    snapshot or the UI."""
    assert _DAG.get_task("send_pod").env_vars == []


def test_the_settings_the_failure_alert_depends_on_are_pinned():
    """Each of these silently degrades the alert while the DAG still looks correct:
    get_logs=False stops the streaming that calls log_formatter at all; deferrable
    defaults to a DEPLOYMENT config lookup whose deferred path writes logs without the
    formatter; log_pod_spec_on_failure prepends the whole pod object, burying the tail
    the rest of this design exists to surface."""
    pod = _DAG.get_task("send_pod")
    assert pod.get_logs is True
    assert pod.deferrable is False
    assert pod.log_pod_spec_on_failure is False
    # The diagnosis the pod spec would have carried (OOMKilled, evictions) has to keep
    # reaching the task log once it is out of the exception.
    assert pod.log_events_on_failure is True


def test_the_dag_stays_one_task():
    """The dbt freshness gate was deliberately removed: a stale mart is self-correcting
    because yesterday's payloads are already logged, so the diff comes up empty. A
    second task reappearing here is that decision being undone by accident."""
    assert _DAG.task_ids == ["send_pod"]


def test_dag_supplies_nothing_retl_will_not_read():
    """The DAG must not invent an env var retl's own config never looks at."""
    module = _dag_module()
    with _pod_runtime(module):
        env = module._reverse_etl_pod_env()
    assert set(env) - _declared_env_vars() == set()
    assert env["RETL_FLOW_HUBSPOT_KEY_COLUMN"] == "gp_person_id"


def test_dag_omits_exactly_the_three_vars_this_deployment_does_not_need():
    """DATABRICKS_TOKEN (the OAuth client id/secret pair is used instead),
    RETL_HUBSPOT_BASE_URL (defaults to the production portal), RETL_CSV_OUTPUT_PATH
    (the other destination). Failure caught: retl adds a FOURTH required env var and
    the DAG never supplies it, so the pod dies at config load on the first run after
    deploy."""
    module = _dag_module()
    with _pod_runtime(module):
        env = module._reverse_etl_pod_env()
    assert _declared_env_vars() - set(env) == {
        "DATABRICKS_TOKEN",
        "RETL_HUBSPOT_BASE_URL",
        "RETL_CSV_OUTPUT_PATH",
    }


def test_the_pod_requests_the_same_oauth_scopes_as_the_tasks_beside_it():
    """The pod does its own token exchange, so a deployment that narrowed its service
    principal refuses the pod while the task's own warehouse queries still succeed.
    Sourced from the connection rather than read separately, so the two cannot drift;
    always present, so the pod's env surface does not change shape with a Variable."""
    module = _dag_module()
    narrowed = {**_FAKE_CONN_FIELDS, "scopes": ["sql", "unity-catalog"]}
    with (
        patch.object(module, "Variable", autospec=True) as mock_variable,
        patch.object(module, "conn_kwargs", autospec=True, return_value=narrowed),
    ):
        mock_variable.get.side_effect = lambda name, default=_NO_DEFAULT: _FAKE_VARIABLES.get(name, "")
        assert module._reverse_etl_pod_env()["DATABRICKS_SCOPES"] == "sql, unity-catalog"
    with _pod_runtime(module):  # the fixture leaves scopes None, i.e. nothing narrowed
        assert module._reverse_etl_pod_env()["DATABRICKS_SCOPES"] == ""


def test_pull_secret_set_attaches_exactly_one_reference():
    """The package is private, so a real deployment pulls with the Astro-provisioned
    secret named by the Variable."""
    module = _dag_module()
    op = module._send_pod()
    with _pod_runtime(module, pull_secret="reverse-etl-ghcr-pull"):
        op.pre_execute({})
    assert len(op.image_pull_secrets) == 1
    assert op.image_pull_secrets[0].name == "reverse-etl-ghcr-pull"


def test_pull_secret_unset_leaves_the_pod_pulling_anonymously():
    module = _dag_module()
    op = module._send_pod()
    assert op.image_pull_secrets == []
    with _pod_runtime(module):
        op.pre_execute({})
    assert op.image_pull_secrets == []


def test_pod_failure_carries_the_captured_log_tail():
    """A bare pod-failed exception is the KPO analog of a bare exit code: an operator
    must see what the pod actually printed without opening the pod log UI."""
    module = _dag_module()
    op = _op_with_tail(module, ["retl FAILED: boom"])
    with (
        _cleanup_failure(module, rows_logged=0),
        pytest.raises(module.AirflowException, match="retl FAILED: boom"),
    ):
        op.cleanup(pod=MagicMock(), remote_pod=MagicMock(), context=_fake_context())


def test_died_at_batch_1_and_died_at_batch_700_read_differently():
    """retl's summary line prints only at the very end of a run, so the tail alone is
    one identical 'retl FAILED' line whether the run died at batch 1 or batch 700 — the
    logged-row count is the only thing that tells the two apart."""
    module = _dag_module()
    texts = []
    for rows in (1, 700):
        op = _op_with_tail(module, ["retl FAILED: boom"])
        with _cleanup_failure(module, rows_logged=rows):
            try:
                op.cleanup(pod=MagicMock(), remote_pod=MagicMock(), context=_fake_context())
            except module.AirflowException as exc:
                texts.append(str(exc))
    assert texts[0] != texts[1]
    assert "partial progress: 1 row(s) logged" in texts[0]
    assert "partial progress: 700 row(s) logged" in texts[1]


def test_probe_failure_does_not_replace_the_pod_failure():
    """The probe is annotating a failure that already happened; its own error must
    never swallow the original one."""
    module = _dag_module()
    op = _op_with_tail(module, ["retl FAILED: boom"])
    with (
        patch.object(
            module.KubernetesPodOperator,
            "cleanup",
            side_effect=module.AirflowException("Pod x returned a failure."),
        ),
        patch.object(module, "_rows_logged_since", side_effect=RuntimeError("warehouse unreachable")),
        _pod_runtime(module),
        pytest.raises(module.AirflowException) as excinfo,
    ):
        op.cleanup(pod=MagicMock(), remote_pod=MagicMock(), context=_fake_context())
    text = str(excinfo.value)
    assert "Pod x returned a failure." in text
    assert "retl FAILED: boom" in text
    assert "RuntimeError" in text and "warehouse unreachable" in text


def test_skip_exception_passes_through_unwrapped():
    """AirflowSkipException subclasses AirflowException; catching the parent alone
    would convert a legitimate skip (skip_on_exit_code) into a failure."""
    module = _dag_module()
    op = module._send_pod()
    with (
        patch.object(
            module.KubernetesPodOperator, "cleanup", side_effect=module.AirflowSkipException("skip")
        ),
        pytest.raises(module.AirflowSkipException),
    ):
        op.cleanup(pod=MagicMock(), remote_pod=MagicMock())


def test_tee_returns_the_providers_own_default_formatting():
    """log_formatter's return value is what the provider writes to the task log, so a
    tee that reformats would silently change the live log an operator reads."""
    assert _dag_module()._send_pod()._tee_log_line("retl", "hello") == "[retl] hello"


def test_a_chatty_run_keeps_only_its_last_lines_in_the_alert():
    """Bounded so a chatty run cannot produce an unusable alert: the lines that matter
    are the ones just before the pod died, so the earliest must fall out."""
    module = _dag_module()
    op = _op_with_tail(module, [f"line {i}" for i in range(module.POD_LOG_TAIL_LINES + 10)])
    with _cleanup_failure(module, rows_logged=0), pytest.raises(module.AirflowException) as excinfo:
        op.cleanup(pod=MagicMock(), remote_pod=MagicMock(), context=_fake_context())
    text = str(excinfo.value)
    assert "line 0" not in text
    assert f"line {module.POD_LOG_TAIL_LINES + 9}" in text


def test_tail_text_is_truncated_to_a_bounded_character_count():
    """40 lines of a chatty stack trace can still be huge; the failure text must stay
    a usable size even when the line cap alone does not bound it."""
    module = _dag_module()
    op = _op_with_tail(module, ["x" * 500 for _ in range(module.POD_LOG_TAIL_LINES)])
    with (
        _cleanup_failure(module, rows_logged=0),
        pytest.raises(module.AirflowException) as excinfo,
    ):
        op.cleanup(pod=MagicMock(), remote_pod=MagicMock(), context=_fake_context())
    assert len(str(excinfo.value)) < module.POD_LOG_TAIL_CHARS + 500


def test_a_hanging_probe_is_abandoned_rather_than_delaying_the_failure():
    """The connector's socket timeout and internal retry budget are both 900s, so a
    probe against an unhealthy warehouse could hold the already-failed task for
    minutes — and hold the one run slot with it."""
    module = _dag_module()
    started = threading.Event()

    def _never_returns(*_args):
        started.set()
        time.sleep(30)

    with (
        patch.object(module, "_rows_logged_since", side_effect=_never_returns),
        patch.object(module, "PROBE_TIMEOUT_SECONDS", 0.1),
    ):
        op = _op_with_tail(module, ["retl FAILED: boom"])
        with (
            patch.object(
                module.KubernetesPodOperator,
                "cleanup",
                side_effect=module.AirflowException("Pod x returned a failure."),
            ),
            _pod_runtime(module),
            pytest.raises(module.AirflowException) as excinfo,
        ):
            op.cleanup(pod=MagicMock(), remote_pod=MagicMock(), context=_fake_context())
    assert started.is_set()
    text = str(excinfo.value)
    assert "Pod x returned a failure." in text
    assert "TimeoutError" in text


def test_rows_logged_since_binds_the_since_parameter_by_name():
    """Catches a marker/key mismatch (e.g. querying `:since` but binding `sent_since`)
    that would otherwise only surface against a live warehouse, at the worst possible
    time — while a run is already failing."""
    module = _dag_module()
    cursor = MagicMock()
    cursor.__enter__.return_value = cursor
    cursor.fetchone.return_value = (7,)
    connection = MagicMock()
    connection.cursor.return_value = cursor
    since = datetime(2026, 9, 9, 17, 0, 0, tzinfo=UTC)
    with (
        patch.object(module, "get_databricks_connection", return_value=connection) as mock_connect,
        patch.object(module, "conn_kwargs", return_value=_FAKE_CONN_FIELDS),
    ):
        result = module._rows_logged_since("goodparty_data_catalog.reverse_etl.sent_log_hubspot", since)
    assert result == 7
    sql, params = cursor.execute.call_args.args
    assert ":since" in sql
    assert params == {"since": since}
    assert mock_connect.call_args.kwargs["max_retries"] == module.PROBE_MAX_RETRIES
    connection.close.assert_called_once()


def _dag_module():
    """The (hash-prefixed, DagBag-assigned) module the DAG file was imported under,
    recovered via the pod operator's own class rather than a fixed name."""
    return sys.modules[type(_DAG.get_task("send_pod")).__module__]


_FAKE_VARIABLES = {
    "reverse_etl_hubspot_token": "tok-123",
    "reverse_etl_hubspot_source_relation": "goodparty_data_catalog.mart_sales_reverse_etl.hubspot",
    "reverse_etl_hubspot_excluded_columns": "added_to_mart_at",
    "reverse_etl_hubspot_cap": "80000",
    "reverse_etl_hubspot_log_table": "goodparty_data_catalog.reverse_etl.sent_log_hubspot",
}
_FAKE_CONN_FIELDS = {
    "host": "https://dbc-fake.cloud.databricks.com",
    "http_path": "/sql/1.0/warehouses/fake",
    "client_id": "fake-client-id",
    "client_secret": "fake-client-secret",
    "scopes": None,
}
_NO_DEFAULT = object()


@contextmanager
def _pod_runtime(module, *, pull_secret=""):
    """Patch what _ReverseEtlPodOperator.pre_execute (and _reverse_etl_pod_env) reach
    for at task runtime, so no real Airflow Variable or Databricks connection is
    needed."""

    def fake_get(name, default=_NO_DEFAULT):
        if name == module.IMAGE_PULL_SECRET_VARIABLE:
            return pull_secret
        if name in _FAKE_VARIABLES:
            return _FAKE_VARIABLES[name]
        if default is not _NO_DEFAULT:
            return default
        raise KeyError(name)

    with (
        patch.object(module, "Variable", autospec=True) as mock_variable,
        patch.object(module, "conn_kwargs", autospec=True, return_value=_FAKE_CONN_FIELDS),
    ):
        mock_variable.get.side_effect = fake_get
        yield


def _op_with_tail(module, lines):
    op = module._send_pod()
    for line in lines:
        op._tee_log_line("retl", line)
    return op


@contextmanager
def _cleanup_failure(module, *, rows_logged):
    """Simulate the provider's cleanup() raising a pod failure, with the
    partial-progress probe stubbed to a canned row count."""
    with (
        patch.object(
            module.KubernetesPodOperator,
            "cleanup",
            side_effect=module.AirflowException("Pod x returned a failure."),
        ),
        patch.object(module, "_rows_logged_since", return_value=rows_logged),
        _pod_runtime(module),
    ):
        yield


def _fake_context():
    return {"dag_run": SimpleNamespace(start_date=datetime(2026, 9, 9, 17, 0, 0, tzinfo=UTC))}
