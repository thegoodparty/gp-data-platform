---
name: dependabot-sweep
description: Triage the repo's open Dependabot alerts and turn the real ones into a single fix PR. Collapses the alert list by package (one CVE against four manifests is one fix), separates alerts against live manifests from alerts against files that no longer exist, resolves the target version, and checks the transitive parent's constraint allows it before promising a bump. Use when asked to look at, clear, sweep, or open a PR for the Dependabot / security alerts, or when handed a link to the repo's dependabot alerts page.
---

# Dependabot sweep

The alert dashboard overstates the work. A single advisory is filed once per
manifest it can reach, and this repo has multi-project locks, so one upgrade
routinely shows up as eight alerts. A second chunk of the list is usually dead:
alerts against manifests that were deleted months ago and never got auto-closed.

The job is to collapse that list down to the actual fixes, ship them as one PR,
and hand the dead alerts back to the user, who closes them in the UI. Do not
dismiss alerts via the API yourself; that is the user's call and needs a token
scope you probably do not have.

## 1. Pull the alerts

```bash
gh api "repos/thegoodparty/gp-data-platform/dependabot/alerts?state=open&per_page=100" \
  --paginate > /tmp/alerts.json
jq 'length' /tmp/alerts.json
```

## 2. Collapse by package

Never work the list alert by alert. Group it, so each row is one decision:

```bash
jq -r 'group_by(.dependency.package.name)
  | map({
      pkg: .[0].dependency.package.name,
      count: length,
      max_sev: (map(.security_advisory.severity)
        | if index("critical") then "critical"
          elif index("high") then "high"
          elif index("medium") then "medium"
          else "low" end),
      target: (map(.security_vulnerability.first_patched_version.identifier // "none")
        | unique | join(",")),
      manifests: (map(.dependency.manifest_path) | unique | join(" "))
    })
  | sort_by(-.count)
  | .[] | "\(.pkg)\t\(.count)\t\(.max_sev)\tneed>=\(.target)\t\(.manifests)"' /tmp/alerts.json
```

The `target` column is every `first_patched_version` across that package's
advisories. If they differ, the fix is the **highest** one, since a bump to the
lowest leaves the other advisories open.

## 3. Split live manifests from dead ones

This is the step that decides how much real work there is. An alert whose
`manifest_path` is not tracked in git has no code fix, because the file is gone.

```bash
git ls-files > /tmp/tracked.txt
jq -r '.[].dependency.manifest_path' /tmp/alerts.json | sort -u | while read -r m; do
  grep -qxF "$m" /tmp/tracked.txt && echo "LIVE  $m" || echo "DEAD  $m"
done
```

For each dead manifest, find when it went away so the report to the user has a
reason attached rather than just an assertion:

```bash
git log --oneline -3 -- <dead-manifest-path>
```

The uv standardization (PRs #466 through #468, June 2026) retired `poetry.lock` in
several subprojects plus a root `requirements_test.txt`, and left their alerts open.
Expect that family of alerts to keep showing up until someone closes them.

Alerts on dead manifests are **not** part of the PR. Collect them for step 6.

### Why they persist, and what does not fix it

`.github/dependabot.yml` is **not** the cause and editing it will not help. That
file configures version-update PRs; security alerts come from the dependency
graph, which is a separate store. The config has carried no `poetry` ecosystem
since the standardization; it is seven `uv` entries plus one `pip` for
`/airflow/astro`.

The dependency graph is what still holds the deleted manifests. Confirm before
proposing any fix:

```bash
gh api graphql -H 'Accept: application/vnd.github.hawkgirl-preview+json' -f query='
{ repository(owner:"thegoodparty", name:"gp-data-platform") {
    dependencyGraphManifests(first:100) { nodes { filename } } } }' \
  --jq '.data.repository.dependencyGraphManifests.nodes[].filename' | sort
```

Entries here that `git ls-files` does not have are stale graph rows. GitHub is
meant to drop them when the file leaves the default branch and sometimes does not.
There is no API to delete one. The remedies are to dismiss the alerts as
`not_used`, or for a repo admin to toggle the dependency graph off and back on in
Settings > Code security, which forces a full re-scan. Both are the owner's call.

## 4. Confirm each live bump is reachable

Most flagged packages are transitive, so the constraint that matters belongs to
the parent, not to us. Check it before committing to a version.

Set the package name once, as a real shell variable. Every command from here on
reads `$PKG`. The searches below all report "not found" as empty output, so a
placeholder left unsubstituted would look exactly like a clean result:

```bash
PKG=sqlparse   # the package from step 2
```

Find the parent in the lock. Scan **every** tracked lock, not just the ones the
alerts named. There are seven uv subprojects (see the multi-venv table in the root
`CLAUDE.md`), and a package can be reachable in one that Dependabot did not flag:

```bash
python3 - "$PKG" <<'PY'
import re, subprocess, sys
pkg = sys.argv[1]
locks = subprocess.run(["git", "ls-files", "*uv.lock"], capture_output=True, text=True).stdout.split()
for lf in locks:
    for block in open(lf).read().split("[[package]]"):
        name = re.search(r'name = "([^"]+)"', block)
        if name and re.search(r'\{ name = "' + re.escape(pkg) + '"', block):
            print(lf, "->", name.group(1))
PY
```

A lock with no hit does not carry the package at all and needs no bump. Confirm
that rather than assuming it from the alert list. Empty output across every lock
means either that, or a typo in `$PKG`; check `$PKG` before believing it.

Then read that parent's own requirement from PyPI, using the parent and version
the previous command printed:

```bash
PARENT=apache-airflow-providers-common-sql; PARENT_VERSION=2.0.1
curl -s "https://pypi.org/pypi/$PARENT/$PARENT_VERSION/json" \
  | python3 -c "import json,sys; pkg=sys.argv[1]; print([r for r in (json.load(sys.stdin)['info'].get('requires_dist') or []) if pkg in r])" "$PKG"
```

If the parent caps the version below the patched release, the lock bump will not
resolve. That is a different job: upgrade the parent, or report that the fix is
blocked upstream. Say so rather than forcing it.

Also check whether our own `pyproject.toml` already pins the package at or above
the patched version. If it does and the alert only names dead manifests, there is
nothing to fix; it belongs in step 6.

### The alert list does not cover the deploy path

Dependabot only sees manifests that name the package. It cannot see a transitive
dependency baked into a base image, so **fixing every alert does not mean
production is patched**. Ask separately how each affected subproject ships.

`airflow/` is the live example. Its `uv.lock` governs local dev only; production
is Astro Runtime, built from `airflow/astro/Dockerfile` plus
`airflow/astro/requirements.txt` (the root `CLAUDE.md` says so). The runtime image
carries its own `apache-airflow` and providers pinned to the Airflow constraints
file, so a lock bump leaves the container untouched.

Check the image directly rather than assuming. It is amd64, so on an arm64 Mac you
cannot execute anything inside it, but you can still read its filesystem:

```bash
IMAGE=$(awk '/^FROM /{print $2}' airflow/astro/Dockerfile)
cid=$(docker create --platform linux/amd64 "$IMAGE")
docker export "$cid" > /tmp/img.tar; docker rm -f "$cid"
mkdir -p /tmp/meta && tar -xf /tmp/img.tar -C /tmp/meta '*.dist-info/METADATA'
grep -rh -i "$PKG" /tmp/meta --include=METADATA | grep -i '^Requires-Dist' | sort -u
grep -rl -i "$PKG" /tmp/meta --include=METADATA | sed 's|/METADATA||;s|.*/||' | sort -u
```

The first grep gives every constraint on the package anywhere in the image, which
tells you whether raising the floor can resolve. The second names the installed
version and which packages pull it. Delete the tar afterwards; it is ~1GB.

If the image ships a vulnerable version, add an explicit floor to
`astro/requirements.txt`. Just the requirement line, no explanatory comment; the
pin says what it does, and the reasoning belongs in the PR body. Do not try to `docker build` the
image locally on arm64 to confirm; the base image's install step is an amd64 binary
and fails with `exec format error`.

## 5. Fix and open one PR

Bump only the flagged package. A bare `uv lock --upgrade` drags in unrelated
churn and turns a reviewable security patch into a large diff.

```bash
cd <subproject> && uv lock --upgrade-package <pkg>
```

Repeat per affected subproject. Transient 502s from the PyPI CDN happen; just
retry the same command.

Verify the diff is only what you intended:

```bash
git diff --stat
git diff -U0 -- '*uv.lock' | grep -E '^[+-]version'
```

Then sync, confirm the installed version, and run each touched subproject's
suite in its own environment (per the multi-venv rule in the root `CLAUDE.md`):

```bash
cd <subproject> && uv sync && uv run python -c "import <pkg>; print(<pkg>.__version__)"
cd <subproject> && uv run pytest -q
```

One PR covers every subproject, since it is one advisory set. Branch as
`chore/<pkg>-security-bump`; these sweeps have no ClickUp ticket, so the
`data-XXXX/` convention does not apply. Hand off to the `pull-request` skill to
open it and drive the delegate reviewer.

The PR body should name each advisory it closes (GHSA id, severity, one-line
summary), state the version moved from and to, and record the test results. Note
which alerts are **not** addressed and why, so a reviewer checking the dashboard
against the diff does not think something was missed.

## 6. Report the dead alerts to the user

End by listing them explicitly, because the user closes these by hand. Give
alert numbers, package, dead manifest, and the reason:

```bash
jq -r '.[] | "\(.number)\t\(.dependency.package.name)\t\(.dependency.manifest_path)\t\(.security_advisory.ghsa_id)"' \
  /tmp/alerts.json | sort -k3
```

`not_used` is the right dismissal reason for a manifest that no longer exists.
