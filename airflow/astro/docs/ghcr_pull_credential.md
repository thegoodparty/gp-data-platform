# The GHCR pull credential (rotation runbook)

The GitHub token behind the `ghcr-pull` Kubernetes secret expires roughly yearly. When it
lapses, every private container-image pull in both Astro deployments fails at once
(`ImagePullBackOff`), pods never start, and nothing degrades gracefully or warns at pull
time. This page is written so an operator can run the rotation cold.

## Prerequisites

- Access to the shared 1Password item "eng-admin github system user" (account password,
  TOTP, current token, expiry date).
- Docker CLI locally (for the pre-flight pull).
- Ability to open an Astronomer support ticket for the deployments.
- A private browser window for the GitHub login.

## What this credential is

- A GitHub machine account, `goodparty-system` (inbox: eng-admin@goodparty.org).
- Its token is a classic personal access token with the single scope `read:packages` and a
  one-year expiry (first minted 2026-09-08). Keep a shared calendar reminder about two
  weeks before each expiry.
- The token backs the Kubernetes docker-registry secret named `ghcr-pull` in BOTH
  Astronomer deployment namespaces (astro-dev and astro-prod). Astronomer support creates
  and updates that secret; it is not self-serve on Astro Hosted.

## What depends on it

Every DAG in this project that launches KubernetesPodOperator pods pulling private images
from ghcr.io: the matcha entity-resolution DAG, the gold-match daily DAG, and the
reverse-ETL DAG. Each references the secret by name through its own Airflow Variable (for
example `gold_match_image_pull_secret`), all pointing at `ghcr-pull`. New packages
published by CI are readable by the account automatically (packages inherit repository
access; verified 2026-09-09 by pulling a package created after the token was minted).

## Rotation procedure (about 30 minutes hands-on, plus support turnaround)

1. Sign in to GitHub as `goodparty-system` in a private browser window.
2. Settings, Developer settings, Personal access tokens, Tokens (classic): generate a new
   token with the single scope `read:packages`, one-year expiry, named self-datingly, for
   example "astro-ghcr-pull, rotate by <month year>".
3. Save the new token and its exact expiry into the 1Password item, and move the shared
   calendar reminder to about two weeks before the new expiry.
4. Pre-flight locally, so any access problem is found before support gets involved:

   ```bash
   echo "$TOKEN" | docker login ghcr.io -u goodparty-system --password-stdin
   docker pull ghcr.io/thegoodparty/gp-data-platform/gold-match:latest
   docker logout ghcr.io
   ```

5. Build the credentials payload directly. Do not send a laptop's own Docker config file:
   Docker Desktop on macOS stores credentials in the keychain, so that file contains no
   usable auth.

   ```bash
   printf '{"auths":{"ghcr.io":{"auth":"%s"}}}' \
     "$(printf 'goodparty-system:%s' "$TOKEN" | base64)" > /tmp/ghcr-config.json
   ```

   The `auth` value is base64 of `username:token`. That is the file format (encoding, not
   encryption); the secrecy comes from the next step.
6. Paste the file's content into a one-time secret at https://ots.astro-cre.com and copy
   the link. Do NOT open the link yourself, the first read burns it. Delete
   `/tmp/ghcr-config.json` immediately after.
7. Open an Astronomer support ticket asking them to UPDATE the existing `ghcr-pull` secret
   in both deployment namespaces from the one-time-secret link (precedent: ticket #98163;
   secrets are namespace and deployment scoped; typical turnaround is same-day).
8. Verify the pull WITHOUT running any application: ask Astronomer support to test-pull an
   image in each namespace, or launch a pod from the image with a no-op command override
   (for example `python -c "print('pull ok')"`). Do NOT trigger a real container DAG for
   this — the matchers write production tables regardless of which deployment launched
   them. Only after a verified pull, revoke the OLD token on the account and note the
   rotation date in the 1Password item.

## Constraints (do not "improve" these away)

- GHCR accepts ONLY classic personal access tokens for registry pulls. Fine-grained tokens
  and GitHub App installation tokens are refused at pull time (a login can appear to
  succeed and the pull still fails "denied"). Verified 2026-09-08 against GitHub's docs and
  community reports.
- IAM-style pulls do not exist for GHCR ("we cannot communicate with GitHub Container
  Registry via IAM", per Astronomer support, 2026-09-08). The static secret is the
  supported path on Astro Hosted.
- Astro namespace secrets are support-managed: no self-serve create, update, or rotation.
- The account is a GitHub Terms-of-Service "machine account" (an explicitly permitted
  category: a human owner is responsible, and it is used exclusively for automation). It
  occupies one paid org seat. Future automation should reuse this account with additional,
  separately scoped tokens rather than creating another account.
- A pod failing on Databricks OAuth scopes is NOT a registry problem: the deployment-wide
  `databricks_scopes` Variable is forwarded into gold-match pods as `DATABRICKS_SCOPES`,
  and a scope refusal at the token endpoint means that Variable and the service
  principal's secret disagree. Different credential, different fix.
