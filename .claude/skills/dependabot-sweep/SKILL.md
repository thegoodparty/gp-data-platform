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
and report the dead alerts separately. Dismissing those is the owner's call, so
ask before doing it, but you can carry it out once they say yes: the repo is
public, so a `repo`-scoped token is enough and no `security_events` scope is
needed. See step 3 for the command and the caveat.

## 0. Set up, and read this first

Almost every search in this skill reports "not found" as **empty output**. A
mistyped package name, an unset variable, or a command run from the wrong
directory therefore produces a result indistinguishable from a genuine all-clear.
Two habits prevent a silent false negative:

- Set the variables below once and let every command read them. Never paste an
  angle-bracket placeholder into a command.
- Never rely on the current directory. Every path here is anchored to `$ROOT`.

```bash
ROOT=$(git rev-parse --show-toplevel)
PKG=          # the package from step 2, e.g. sqlparse
: "${PKG:?set PKG before running the commands below}"
```

The `:?` line aborts loudly if `PKG` is empty, which is the point. Re-run it after
setting `PKG` and expect no output.

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

`manifest_path` is repo-root-relative, so the tracked-file list must be too. Note
the `git -C "$ROOT"`: plain `git ls-files` run from a subproject lists only that
subtree, which would mark every manifest DEAD and silently reduce the sweep to
nothing.

```bash
git -C "$ROOT" ls-files > /tmp/tracked.txt
jq -r '.[].dependency.manifest_path' /tmp/alerts.json | sort -u | while read -r m; do
  grep -qxF "$m" /tmp/tracked.txt && echo "LIVE  $m" || echo "DEAD  $m"
done
```

For each dead manifest, find when it went away so the report to the user has a
reason attached rather than just an assertion:

```bash
jq -r '.[].dependency.manifest_path' /tmp/alerts.json | sort -u | while read -r m; do
  grep -qxF "$m" /tmp/tracked.txt || {
    printf '%-24s last touched in ' "$m"
    git -C "$ROOT" log -1 --format='%h %ad %s' --date=short -- "$m"
  }
done
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
The `poetry.lock` rows survived every push to main for over two months, including
a merge that touched the graph's own manifests.

There is no API to delete a graph row, and **this repo is public, so the
dependency graph cannot be turned off**. The off/on toggle in Settings > Code
security that people suggest for this does not exist here; `security_and_analysis`
carries no `dependency_graph` key, and a `PATCH` setting one is silently ignored.
Do not send anyone looking for it.

That leaves dismissal, which needs no extra token scope on a public repo (`repo`
covers it). Confirm with the user first, then:

```bash
gh api -X PATCH "repos/thegoodparty/gp-data-platform/dependabot/alerts/<n>" \
  -f state=dismissed -f dismissed_reason=not_used \
  -f dismissed_comment="Manifest <path> was deleted in #<pr>. The dependency graph still lists it; there is no code to patch." \
  --jq '{number, state, dismissed_reason}'
```

`not_used` is the right reason: the vulnerable code is genuinely not reachable
because the manifest is gone. Dismissals are reversible from the UI.

Be honest that this treats the symptom. The graph row survives, so a **new**
advisory against a package in a dead manifest can file a fresh alert against the
same path later. If that happens, the escalation is a GitHub Support ticket
asking them to re-index the repository's dependency graph, not another sweep.

## 4. Confirm each live bump is reachable

Most flagged packages are transitive, so the constraint that matters belongs to
the parent, not to us. Check it before committing to a version.

Find the parent. Scan **every** tracked lock, not just the ones the alerts named.
There are seven uv subprojects (see the multi-venv table in the root `CLAUDE.md`),
and a package can be reachable in one that Dependabot did not flag. The scan is
anchored to `$ROOT` for the same reason step 3 is:

```bash
python3 - "$ROOT" "$PKG" <<'PY'
import os, re, subprocess, sys
root, pkg = sys.argv[1], sys.argv[2]
locks = subprocess.run(["git", "ls-files", "--full-name", ":(glob,top)**/uv.lock"],
                       capture_output=True, text=True, cwd=root).stdout.split()
print(f"scanned {len(locks)} locks for {pkg!r}")
for lf in locks:
    for block in open(os.path.join(root, lf)).read().split("[[package]]"):
        name = re.search(r'name = "([^"]+)"', block)
        ver = re.search(r'version = "([^"]+)"', block)
        if name and name.group(1) != pkg and re.search(r'\{ name = "' + re.escape(pkg) + '"', block):
            print(f"SUBPROJECT={os.path.dirname(lf) or '.'} "
                  f"PARENT={name.group(1)} PARENT_VERSION={ver.group(1) if ver else '?'}")
PY
```

It prints the lock count first, so "scanned 7 locks" with no rows below it means
the package genuinely is not there, rather than the scan having failed. A lock with
no hit needs no bump; confirm that rather than assuming it from the alert list.

Each row is emitted as ready-to-paste shell assignments, so nothing has to be
transcribed or trimmed by hand:

```
scanned 7 locks for 'sqlparse'
SUBPROJECT=airflow PARENT=apache-airflow-providers-common-sql PARENT_VERSION=2.0.1
SUBPROJECT=dbt PARENT=apache-airflow-providers-common-sql PARENT_VERSION=2.0.1
```

Then read that parent's own requirement from PyPI. Copy the assignments from the
scan output rather than typing them; `curl -f` turns a wrong parent or version
into a visible HTTP error instead of a quiet empty list:

```bash
PARENT=            # PARENT= value printed by the scan above
PARENT_VERSION=    # PARENT_VERSION= value printed by the scan above
: "${PARENT:?}" "${PARENT_VERSION:?}"
curl -fsS "https://pypi.org/pypi/$PARENT/$PARENT_VERSION/json" \
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
IMAGE=$(awk '/^FROM /{print $2}' "$ROOT/airflow/astro/Dockerfile")
cid=$(docker create --platform linux/amd64 "$IMAGE")
docker export "$cid" > /tmp/img.tar; docker rm -f "$cid"
mkdir -p /tmp/meta && tar -xf /tmp/img.tar -C /tmp/meta '*.dist-info/METADATA'
grep -rh -i "$PKG" /tmp/meta --include=METADATA | grep -i '^Requires-Dist' | sort -u
grep -rl -i "$PKG" /tmp/meta --include=METADATA | sed 's|/METADATA||;s|.*/||' | sort -u
rm -rf /tmp/img.tar /tmp/meta
```

The first grep gives every constraint on the package anywhere in the image, which
tells you whether raising the floor can resolve. The second names the installed
version and which packages pull it. Delete the tar afterwards; it is ~1GB.

If the image ships a vulnerable version, add an explicit floor to
`astro/requirements.txt`. Just the requirement line, no explanatory comment; the
pin says what it does, and the reasoning belongs in the PR body. Do not try to
`docker build` the image locally on arm64 to confirm; the base image's install step
is an amd64 binary and fails with `exec format error`.

## 5. Fix and open one PR

Bump only the flagged package. A bare `uv lock --upgrade` drags in unrelated
churn and turns a reviewable security patch into a large diff.

Use the `SUBPROJECT=` assignment the step 4 scan printed, and repeat the block per
affected subproject. It is the directory (`airflow`), not the lock path
(`airflow/uv.lock`). The `cd` runs in a subshell so `$ROOT` stays valid afterwards:

```bash
SUBPROJECT=        # SUBPROJECT= value printed by the step 4 scan, e.g. airflow
: "${SUBPROJECT:?}"
[ -d "$ROOT/$SUBPROJECT" ] || echo "not a directory: $ROOT/$SUBPROJECT"
( cd "$ROOT/$SUBPROJECT" && uv lock --upgrade-package "$PKG" )
```

Transient 502s from the PyPI CDN happen; just retry the same command.

Verify the diff is only what you intended:

```bash
git -C "$ROOT" diff --stat
git -C "$ROOT" diff -U0 -- '*uv.lock' | grep -E '^[+-]version'
```

Then sync, confirm the installed version, and run each touched subproject's
suite in its own environment (per the multi-venv rule in the root `CLAUDE.md`):

```bash
( cd "$ROOT/$SUBPROJECT" && uv sync && uv run python -c "import $PKG; print($PKG.__version__)" )
( cd "$ROOT/$SUBPROJECT" && uv run pytest -q )
```

The import name is not always the package name (`psycopg2-binary` imports as
`psycopg2`, `databricks-sdk` as `databricks.sdk`). If the import fails, check the
name before concluding the sync did not work.

One PR covers every subproject, since it is one advisory set. Branch as
`chore/$PKG-security-bump`; these sweeps have no ClickUp ticket, so the
`data-XXXX/` convention does not apply. Hand off to the `pull-request` skill to
open it and drive the delegate reviewer.

The PR body should name each advisory it closes (GHSA id, severity, one-line
summary), state the version moved from and to, and record the test results. Note
which alerts are **not** addressed and why, so a reviewer checking the dashboard
against the diff does not think something was missed.

## 6. Report the dead alerts to the user

End by listing them explicitly, because the user closes these by hand. Give
alert numbers, package, dead manifest, and the advisory:

```bash
jq -r '.[] | [.number, .dependency.package.name, .dependency.manifest_path, .security_advisory.ghsa_id, .security_advisory.severity] | @tsv' \
  /tmp/alerts.json | while IFS=$'\t' read -r n p m g s; do
    grep -qxF "$m" /tmp/tracked.txt || printf '#%s\t%-15s %-22s %-22s %s\n' "$n" "$p" "$m" "$g" "$s"
  done | sort -V
```

Ask whether they want them dismissed. If yes, run the `PATCH` from step 3 over
this list; if no, leave them and say they will stay on the dashboard.
