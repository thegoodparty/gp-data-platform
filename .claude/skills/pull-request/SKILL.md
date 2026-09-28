---
name: pull-request
description: Open or update a pull request and drive it through the delegate-reviewer bot until it approves — trigger the review, judge each requested change on its merits, push back with evidence on findings that are wrong or overly picky, and report approval plus CI state. Use whenever opening a PR in this repo, pushing an update to one, or asked to get a PR reviewed, approved, or ready to merge.
---

# Pull request

Open the PR, then loop with the delegate reviewer until it approves. The loop is the job; a PR is not done because the code is written.

Conventions for branch names, PR titles, and keeping ticket ids out of committed code live in `dbt/project/CLAUDE.md`. Read it rather than guessing.

## 1. Before you open or update

```bash
pre-commit run --files <changed files>   # sqlfmt reformats; re-run until clean
```

Run the affected test suite too. CI runs `pre-commit run --all-files`, so a skipped hook blocks the merge either way.

PR body: what changed, why it is shaped that way, and a verification section with real numbers from an actual run. Include operational findings a reviewer or the person doing the cutover would need — ordering dependencies, contention, anything that will bite in production.

## 2. Trigger the review

**A brand-new PR triggers `delegate-reviewer` automatically.** Do not comment; you will just queue a second review.

**Every subsequent round needs an explicit trigger**, including after a push to an existing PR:

```bash
gh pr comment <n> --body "delegate review"
```

It must be a **standalone comment** — exactly that text, nothing else in the body. Wrapping it in a sentence or combining it with your reply to a finding will not trigger the bot. When you also want to reply to findings, post the replies first, then `delegate review` as its own separate comment.

## 3. Wait for the feedback

```bash
gh pr view <n> --json reviews --jq '.reviews[-3:][] | "\(.author.login) | \(.state) | \(.submittedAt)"'
gh api repos/thegoodparty/gp-data-platform/pulls/<n>/comments \
  --jq '.[] | {id, user: .user.login, path, line, body}'
```

The first gives review-level verdicts, the second the inline findings, which is where the substance is. Filter by `submittedAt` newer than your trigger so you read this round's review, not the last one's.

Two traps:

- The author login is `delegate-reviewer` in the reviews API but `delegate-reviewer[bot]` in the inline-comments API. Match accordingly.
- `claude[bot]` posts a generic "comment `@claude review`" notice on open, and `cursor` may also comment. Neither is the reviewer; neither needs action.

A round can take several minutes. Poll on an until-loop with a timeout rather than a fixed sleep.

## 4. Judge each finding on its merits

This is the part that needs actual thought. **Do not default to a code change to make the bot go quiet.** A change made only to appease a reviewer adds unjustified code and can introduce a real bug.

**Do not assume the reviewer understands external system behavior.** It regularly reasons incorrectly about Databricks SQL semantics, Spark type coercion, Postgres locking, and dbt materialization behavior. Verify the claim against the real system before you accept it — run the one-line query, read the migration, check the actual lock mode.

A worked example: Databricks `least()`/`greatest()` **skip** nulls rather than propagating them, unlike MySQL. The reviewer flags null-propagation bugs here that do not exist. Rebut with the query that demonstrates it (`select least(null, 'b')` returns `'b'`).

Then, per finding:

- **Real defect** → fix it. Keep the fix the smallest thing that addresses the actual failure.
- **Wrong about system behavior** → reply declining, with the evidence that settles it. Name the behavior and show the query or doc.
- **A corner case that cannot occur** given surrounding constraints → reply declining, and state which constraint rules it out.
- **Style or preference** with no defect behind it → decline briefly. It is sometimes too picky.

Reply as a top-level comment referencing the file and line, which is reliable and visible:

```bash
gh pr comment <n> --body "..."
```

To reply threaded under a specific inline finding, use its comment id:

```bash
gh api repos/thegoodparty/gp-data-platform/pulls/<n>/comments/<comment_id>/replies -f body="..."
```

Answer every finding, one way or the other. A silently ignored finding usually comes back next round.

## 5. Repeat

After pushing fixes or posting declines, trigger again (step 2) and re-read (step 3). Repeat until the review state is approval.

If the same finding returns after you declined it with evidence, do not just re-decline. Either your rebuttal did not land, in which case restate it more concretely, or the reviewer is seeing something you dismissed. Re-check before holding the line.

## 6. Report to the user

Say plainly that the delegate bot approved, and give the CI state alongside it:

```bash
gh pr checks <n>
```

Report each check's real status. If a check is still pending or failing, say so — approval from the bot is not the same as a green PR. Summarize what changed across the review rounds and what you declined, so the user can overrule a decline if they disagree.
