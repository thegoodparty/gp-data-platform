import json

from semantic_catalog.github_client import GitHubClient


class _FakeResp:
    def __init__(self, payload):
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self):
        return json.dumps(self._payload).encode()


def _client(pages):
    """pages: list of successive JSON payloads returned per request, in order."""
    calls = []
    seq = list(pages)

    def fake_urlopen(req, timeout=30):
        calls.append(req)
        return _FakeResp(seq.pop(0))

    return GitHubClient("ghtok", "thegoodparty/gp-data-platform", urlopen=fake_urlopen), calls


def test_pr_files_paginates_until_short_page():
    page1 = [{"filename": f"f{i}.txt"} for i in range(100)]
    page2 = [{"filename": "dbt/project/models/marts/sem_analytics__users_win.yml"}]
    gh, calls = _client([page1, page2])
    files = gh.pr_files(7)
    assert len(files) == 101 and files[-1].endswith("sem_analytics__users_win.yml")
    assert "per_page=100" in calls[0].full_url and "page=1" in calls[0].full_url
    assert "page=2" in calls[1].full_url
    assert calls[0].get_header("Authorization") == "Bearer ghtok"


def test_team_members_extracts_logins():
    gh, _ = _client([[{"login": "alice"}, {"login": "bob"}]])
    assert gh.team_members("thegoodparty", "semantic-layer-data") == ["alice", "bob"]


def test_create_comment_posts_body_and_returns_id():
    gh, calls = _client([{"id": 991}])
    cid = gh.create_comment(7, "marker body")
    assert cid == 991
    assert calls[0].get_method() == "POST"
    assert json.loads(calls[0].data) == {"body": "marker body"}
    assert calls[0].full_url.endswith("/repos/thegoodparty/gp-data-platform/issues/7/comments")


def test_update_comment_patches():
    gh, calls = _client([{"id": 991}])
    gh.update_comment(991, "new body")
    assert calls[0].get_method() == "PATCH"
    assert calls[0].full_url.endswith("/repos/thegoodparty/gp-data-platform/issues/comments/991")
