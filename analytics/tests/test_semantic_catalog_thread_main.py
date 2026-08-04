import json
import urllib.error

import semantic_catalog.github_client as ghmod
import semantic_catalog.slack_reply as slmod
from semantic_catalog import thread


class _FakeResp:
    def __init__(self, payload):
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self):
        return json.dumps(self._payload).encode()


def _script(monkeypatch, responses):
    """Route fake responses by URL substring; record all requests.

    A response entry may pair a fragment with an Exception instance instead of
    a payload; matching that fragment raises it (simulates a transport failure
    such as urllib.error.URLError) instead of returning a fake response.
    """
    calls = []

    def fake_urlopen(req, timeout=30):
        calls.append(req)
        for frag, payload in responses:
            if frag in req.full_url:
                if isinstance(payload, Exception):
                    raise payload
                return _FakeResp(payload)
        raise AssertionError(f"unexpected URL: {req.full_url}")

    monkeypatch.setattr(ghmod.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(slmod.urllib.request, "urlopen", fake_urlopen)
    return calls


def _env(monkeypatch, **over):
    base = dict(
        GH_TOKEN="ghtok",
        ORG_READ_TOKEN="orgtok",
        SLACK_APP_BOT_TOKEN="slacktok",
        SLACK_CHANNEL_ID="C1",
        GITHUB_REPOSITORY="thegoodparty/gp-data-platform",
    )
    base.update(over)
    for k, v in base.items():
        (monkeypatch.delenv(k, raising=False) if v is None else monkeypatch.setenv(k, v))


def _event(tmp_path, **over):
    pr = dict(
        number=7,
        title="Ratify win_users",
        html_url="https://github.com/x/pull/7",
        draft=False,
        state="open",
        merged=False,
    )
    pr.update(over)
    p = tmp_path / "event.json"
    p.write_text(json.dumps({"pull_request": pr}))
    return p


def test_reconcile_skips_non_governed_pr(tmp_path, monkeypatch, capsys):
    _env(monkeypatch)
    calls = _script(monkeypatch, [("/pulls/7/files", [{"filename": "README.md"}])])
    rc = thread.main(["--event-path", str(_event(tmp_path))])
    assert rc == 0
    assert len(calls) == 1  # stopped after the files check
    assert "not governed" in capsys.readouterr().out


def test_reconcile_bootstraps_anchor_and_creates_marker(tmp_path, monkeypatch):
    _env(monkeypatch)
    calls = _script(
        monkeypatch,
        [
            ("/pulls/7/files", [{"filename": "dbt/project/models/marts/sem_analytics__users_win.yml"}]),
            ("/pulls/7/reviews", []),
            ("/teams/semantic-layer-data/members", [{"login": "tristan"}]),
            ("/teams/semantic-layer-business/members", [{"login": "joe"}]),
            ("/issues/7/comments?", []),
            ("chat.postMessage", {"ok": True, "ts": "1722.0042"}),
            ("chat.getPermalink", {"ok": True, "permalink": "https://gp.slack.com/p42"}),
            ("/issues/7/comments", {"id": 991}),  # create marker (POST matches after the paginated GET)
        ],
    )
    rc = thread.main(["--event-path", str(_event(tmp_path))])
    assert rc == 0
    created = [c for c in calls if c.get_method() == "POST" and "/issues/7/comments" in c.full_url]
    assert created, "marker comment must be created"
    body = json.loads(created[-1].data)["body"]
    assert "semantic-layer-thread v1" in body and "1722.0042" in body


def test_reconcile_routes_reviews_and_team_lookups_to_the_right_token(tmp_path, monkeypatch):
    """PR reviews are a workflow-token (GH_TOKEN) concern; team membership is org-token."""
    _env(monkeypatch)
    calls = _script(
        monkeypatch,
        [
            ("/pulls/7/files", [{"filename": "dbt/project/models/marts/sem_analytics__users_win.yml"}]),
            ("/pulls/7/reviews", []),
            ("/teams/semantic-layer-data/members", [{"login": "tristan"}]),
            ("/teams/semantic-layer-business/members", [{"login": "joe"}]),
            ("/issues/7/comments?", []),
            ("chat.postMessage", {"ok": True, "ts": "1722.0042"}),
            ("chat.getPermalink", {"ok": True, "permalink": "https://gp.slack.com/p42"}),
            ("/issues/7/comments", {"id": 991}),
        ],
    )
    rc = thread.main(["--event-path", str(_event(tmp_path))])
    assert rc == 0
    reviews_calls = [c for c in calls if "/pulls/7/reviews" in c.full_url]
    team_calls = [c for c in calls if "/teams/" in c.full_url]
    assert reviews_calls and all(c.get_header("Authorization") == "Bearer ghtok" for c in reviews_calls)
    assert team_calls and all(c.get_header("Authorization") == "Bearer orgtok" for c in team_calls)


def test_reconcile_degrades_when_org_token_lookup_fails(tmp_path, monkeypatch):
    """A narrowly-scoped/expired ORG_READ_TOKEN must degrade, not crash the whole reconcile."""
    _env(monkeypatch)
    calls = _script(
        monkeypatch,
        [
            ("/pulls/7/files", [{"filename": "dbt/project/models/marts/sem_analytics__users_win.yml"}]),
            ("/pulls/7/reviews", []),
            ("/teams/", urllib.error.URLError("no route to host")),
            ("/issues/7/comments?", []),
            ("chat.postMessage", {"ok": True, "ts": "1722.0042"}),
            ("chat.getPermalink", {"ok": True, "permalink": "https://gp.slack.com/p42"}),
            ("/issues/7/comments", {"id": 991}),
        ],
    )
    rc = thread.main(["--event-path", str(_event(tmp_path))])
    assert rc == 0
    posted = [c for c in calls if "chat.postMessage" in c.full_url]
    assert posted, "anchor must still be posted"
    created = [c for c in calls if c.get_method() == "POST" and "/issues/7/comments" in c.full_url]
    assert created, "marker comment must still be created"
    body = json.loads(created[-1].data)["body"]
    from semantic_catalog import pr_marker

    state = pr_marker.parse(body)
    assert state is not None
    assert state.announced == {"data": None, "business": None}  # no approval diff was possible


def test_reconcile_degrades_when_permalink_lookup_raises_transport_error(tmp_path, monkeypatch):
    """A permalink timeout/URLError must not crash after the anchor is posted, or the
    marker never gets written and the next event re-bootstraps a duplicate anchor."""
    _env(monkeypatch)
    calls = _script(
        monkeypatch,
        [
            ("/pulls/7/files", [{"filename": "dbt/project/models/marts/sem_analytics__users_win.yml"}]),
            ("/pulls/7/reviews", []),
            ("/teams/semantic-layer-data/members", [{"login": "tristan"}]),
            ("/teams/semantic-layer-business/members", [{"login": "joe"}]),
            ("/issues/7/comments?", []),
            ("chat.postMessage", {"ok": True, "ts": "1722.0042"}),
            ("chat.getPermalink", urllib.error.URLError("timed out")),
            ("/issues/7/comments", {"id": 991}),
        ],
    )
    rc = thread.main(["--event-path", str(_event(tmp_path))])
    assert rc == 0
    created = [c for c in calls if c.get_method() == "POST" and "/issues/7/comments" in c.full_url]
    assert created, "marker comment must still be created despite the permalink failure"
    body = json.loads(created[-1].data)["body"]
    from semantic_catalog import pr_marker

    state = pr_marker.parse(body)
    assert state is not None
    assert state.permalink == ""


def test_reconcile_merged_close_with_current_marker_writes_nothing(tmp_path, monkeypatch):
    """A merged PR's close event must not touch the comments API at all when the
    marker is already current: any write there (even a no-op PATCH) widens the
    race window against the publish workflow's mark-merged PATCH on the same
    comment (last-writer-wins)."""
    from semantic_catalog import pr_marker
    from semantic_catalog.pr_marker import ThreadState

    _env(monkeypatch)
    marker_body = pr_marker.render(
        ThreadState(ts="1722.0042", channel="C1", permalink="https://p", pr_state="open")
    )
    calls = _script(
        monkeypatch,
        [
            ("/pulls/7/files", [{"filename": "dbt/project/models/marts/sem_analytics__users_win.yml"}]),
            ("/pulls/7/reviews", []),
            ("/teams/semantic-layer-data/members", []),
            ("/teams/semantic-layer-business/members", []),
            ("/issues/7/comments?", [{"id": 991, "body": marker_body}]),
        ],
    )
    rc = thread.main(["--event-path", str(_event(tmp_path, state="closed", merged=True))])
    assert rc == 0
    comment_writes = [
        c
        for c in calls
        if c.get_method() in ("POST", "PATCH")
        and ("/issues/7/comments" in c.full_url or "/issues/comments/" in c.full_url)
    ]
    assert comment_writes == []


def test_reconcile_without_slack_token_skips_cleanly(tmp_path, monkeypatch, capsys):
    _env(monkeypatch, SLACK_APP_BOT_TOKEN=None)
    _script(monkeypatch, [("/pulls/7/files", [{"filename": "dbt/project/models/sem_x.yml"}])])
    rc = thread.main(["--event-path", str(_event(tmp_path))])
    assert rc == 0
    assert "SLACK_APP_BOT_TOKEN" in capsys.readouterr().out


def test_emit_marker_writes_state_json(tmp_path, monkeypatch):
    from semantic_catalog import pr_marker
    from semantic_catalog.pr_marker import ThreadState

    _env(monkeypatch)
    marker_body = pr_marker.render(ThreadState(ts="1722.0042", channel="C1", permalink="https://p"))
    _script(monkeypatch, [("/issues/7/comments?", [{"id": 991, "body": marker_body}])])
    out = tmp_path / "marker.json"
    rc = thread.main(["--emit-marker", str(out), "--pr", "7"])
    assert rc == 0
    d = json.loads(out.read_text())
    assert d == {"ts": "1722.0042", "merged": False, "channel": "C1"}


def test_emit_marker_writes_empty_object_when_absent(tmp_path, monkeypatch):
    _env(monkeypatch)
    _script(monkeypatch, [("/issues/7/comments?", [{"id": 1, "body": "human chatter"}])])
    out = tmp_path / "marker.json"
    assert thread.main(["--emit-marker", str(out), "--pr", "7"]) == 0
    assert json.loads(out.read_text()) == {}


def test_emit_marker_rejects_malformed_ts(tmp_path, monkeypatch, capsys):
    """A forged/corrupt marker with a non-numeric ts must not flow through to
    GITHUB_ENV in the publish workflow (a newline in ts could inject an env
    var); treat it as no-marker instead."""
    from semantic_catalog import pr_marker
    from semantic_catalog.pr_marker import ThreadState

    _env(monkeypatch)
    marker_body = pr_marker.render(ThreadState(ts="x\nFOO=bar", channel="C1", permalink="https://p"))
    _script(monkeypatch, [("/issues/7/comments?", [{"id": 991, "body": marker_body}])])
    out = tmp_path / "marker.json"
    assert thread.main(["--emit-marker", str(out), "--pr", "7"]) == 0
    assert json.loads(out.read_text()) == {}
    assert "warning" in capsys.readouterr().out.lower()


def test_mark_merged_updates_marker(tmp_path, monkeypatch):
    from semantic_catalog import pr_marker
    from semantic_catalog.pr_marker import ThreadState

    _env(monkeypatch)
    marker_body = pr_marker.render(ThreadState(ts="1722.0042", channel="C1", permalink="https://p"))
    calls = _script(
        monkeypatch,
        [
            ("/issues/7/comments?", [{"id": 991, "body": marker_body}]),
            ("/issues/comments/991", {"id": 991}),
        ],
    )
    assert thread.main(["--mark-merged", "--pr", "7"]) == 0
    patch = [c for c in calls if c.get_method() == "PATCH"][-1]
    assert '"merged": true' in json.loads(patch.data)["body"]
