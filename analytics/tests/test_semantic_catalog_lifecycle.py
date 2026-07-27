import subprocess

from semantic_catalog.lifecycle import Lifecycle, derive


def _fake_git(commits):
    # commits: list of (iso_date, subject) newest-first, as `git log` would print.
    log = "\n".join(f"{d}\x1f{s}" for d, s in commits)

    def run_git(args):
        return log

    return run_git


def test_derive_first_and_last():
    run_git = _fake_git(
        [
            ("2026-07-24", "Merge pull request #720 from data-2184/x"),
            ("2026-06-01", "Merge pull request #610 from data-2109/y"),
        ]
    )
    lc = derive("sem_analytics__users_win.yml", run_git=run_git)
    assert lc.last_updated == "2026-07-24"
    assert lc.last_updated_pr == "720"
    assert lc.created == "2026-06-01"
    assert lc.created_pr == "610"


def test_derive_handles_no_pr_in_subject():
    run_git = _fake_git([("2026-07-24", "direct commit no pr")])
    lc = derive("x.yml", run_git=run_git)
    assert lc.created == "2026-07-24"
    assert lc.created_pr is None


def test_derive_handles_untracked_file():
    def run_git(args):
        return ""

    lc = derive("never_committed.yml", run_git=run_git)
    assert lc.created is None
    assert lc.last_updated is None


def test_derive_degrades_on_git_error():
    # git missing / shallow clone / corrupt repo: return an empty lifecycle
    # rather than propagating the exception and crashing the generator.
    def run_git(args):
        raise subprocess.CalledProcessError(128, ["git", "log"])

    assert derive("x.yml", run_git=run_git) == Lifecycle(None, None, None, None)


def test_derive_takes_last_pr_number_in_title():
    # A squash title with a linked issue then the PR: last "(#N)" is the PR.
    run_git = _fake_git([("2026-07-24", "Feature (#609) (#720)")])
    lc = derive("x.yml", run_git=run_git)
    assert lc.created_pr == "720"
    assert lc.last_updated_pr == "720"
