---
name: pr-prep
description: Use when creating a pull request for this project - triggers on "open a PR", "create a PR", "gh pr create", or any request to open/create a PR. Commits pending work, runs parallel CGU + security checks, writes a French PR description, then creates the PR with confirmation.
allowed-tools: Bash, Read, Grep, Glob, Edit, Write, WebFetch, Agent, Skill
---

# PR Preparer

Pre-flight gate that wraps PR creation: commit pending work, run blocking/non-blocking
checks in parallel, write a French PR, then create it with confirmation.

> ## Authorised CLAUDE.md overrides - DO NOT "fix" these
>
> This skill deliberately overrides two repo/global rules, on explicit user instruction:
>
> 1. **It commits.** CLAUDE.md says "Claude cannot commit". Here, committing is the
>    intended behaviour (squash-merge cleans history later, so many local commits are fine).
> 2. **It scrubs attribution.** Global rules say append `Co-Authored-By: Claude` and a
>    "Generated with Claude Code" line. This skill is **undercover**: never add either, and
>    never mention Claude in any commit, branch, or PR text.

## When this runs

Fires on PR-creation intent: the user says "open a PR" / "create a PR", runs `gh pr create`,
or asks me to open one. It is the only path that should create a PR for this project.

## Preconditions

- Must be in a git repo with a working tree. If clean AND no unpushed commits on a feature
  branch -> nothing to do; stop and say so.
- If on `main` with uncommitted changes -> the skill creates the branch itself (step 1).
- Requires `$DATABASE_URL` is NOT needed; requires network for the CGU refresh and `gh`/MCP.

## Procedure

### 1. Branch

- `git branch --show-current`. If on `main`, infer `<type>/<scope-kebab>` from the diff paths
  and `git switch -c <type>/<scope-kebab>` (ASCII, no accents). Otherwise keep current branch.
- Pick a release-appropriate `<type>`/`<scope>` per step 2b when the diff touches app-stack code.

### 2. Commit

- Stage all intended changes (`git add -A`, or a targeted subset if the user scoped it).
- Invoke the **`french`** skill, then write a conventional-commit message:
  `<type>(<scope>): <description en français>` - the `<type>(<scope>)` token stays ASCII
  English (`feat`, `fix`, `chore`, `refactor`, ...); the description is French with proper
  accents. Body in French if useful.
- Commit with **no `Co-Authored-By` trailer** and no Claude mention.
- Many local commits are fine (squash-merge later). Commit before checks so the diff is stable.
- Choose `<type>(<scope>)` **release-aware** per step 2b below - the squash-merge uses the
  PR title as the commit subject, so this token decides whether the app deploys.

### 2b. Release-aware type & scope

The release/deploy pipeline has **two independent gates**; **both** must pass for
semantic-release to cut a version and the app to deploy. Because squash-merge turns the PR
title into the commit subject, the PR title's `<type>(<scope>)` is decisive - get it wrong
and merged app code never ships.

**Gate 1 - file filter** (`.github/workflows/quality.yml`, `changes` job, `dorny/paths-filter`):
sets `app=true` only when the diff touches `api/**`, `app-partners/**`, `app-observatory/**`,
or `shared/**`. The `release` job is gated on `app=true`. A **data-only** PR (sqlmesh / dbt /
datalake / cms / docker / .github ...) can **never** cut a release, whatever the title says.

**Gate 2 - commit-analyzer** (`.releaserc`, `conventionalcommits` preset):

- Releasing types: `feat` -> minor, `fix` / `perf` / `revert` -> patch, `BREAKING CHANGE` (or `!`) -> major.
- Non-releasing types: `chore`, `docs`, `refactor`, `style`, `test`, `build`, `ci` -> **no release**.
- Scope override (`releaseRules`): scope `dbt`, `sqlmesh`, `datalake`, `cms` -> **no release**,
  even with a `feat`/`fix` type.

**Naming rule:**

- Diff touches **app-stack code** (`api` / `app-partners` / `app-observatory` / `shared`) **and
  must deploy** -> title MUST be a releasing type (`feat`/`fix`/`perf`) with a **non-data scope**
  reflecting the app area (e.g. `export`, `api`, `partners`, `observatory`). **Never** scope app
  code with `sqlmesh`/`dbt`/`datalake`/`cms`.
- PR **mixes** app code with data files -> do not use a data scope (it suppresses the release and
  the app code never deploys). Use an app scope, or split the data-only part into its own PR.
- PR is **data-only** or intentionally non-deploying (docs/CI/chore) -> use the honest
  type/scope; it will correctly **not** cut a release.

**Pitfalls (both seen in practice):**

- `fix(sqlmesh)` on a PR that also edits `api/**`: files pass gate 1, but the `sqlmesh` scope
  trips gate 2 -> no release -> API not deployed.
- A data-only PR retitled `fix(api)` to force a release: gate 2 ok, but gate 1 sees no app
  files -> `release` job skipped -> still no release.

**Why it matters:** `deploy-api` / `deploy-partner` / `deploy-observatory` fire on
`release: published` and build an image tagged with the **release tag**, which the cluster
deploys. A manual `workflow_dispatch` builds a `github.sha`-tagged image the cluster does not
reference -> no rollout. A real release is the only way to deploy app code.

### 3. CGU freshness (refresh if stale)

- Read `cgu-rules.md` frontmatter `last_synced`. Compute age with `date`.
- If missing or **older than 7 days** -> mark stale:
  - `WebFetch` the canonical CGU URL in `cgu-rules.md`'s header.
  - Rewrite `cgu-rules.md` from the fetched content, preserving the rule structure (id /
    section / severity / what-to-flag), and restamp `last_synced` to today (`date -I`).
  - Keep human `# TODO extend` markers.
- The CGU guard (step 4) always runs against the refreshed file.

### 4. Parallel checks (registry-driven)

Dispatch **every registry row as a parallel read-only `Explore` subagent in one batch**.
Each subagent reads its `source` checklist + the PR diff
(`git diff $(git merge-base main HEAD)`) and returns a structured report **in French**:
verdict `PASS`/`FAIL`, severity, and findings (`fichier:ligne`, problème, recommandation).

**Check registry** (extend by appending a row - no orchestration change):

| name            | blocking | source (checklist read by the subagent)          |
| --------------- | -------- | ------------------------------------------------ |
| `cgu-guard`     | **yes**  | `.claude/skills/pr-prep/cgu-rules.md`        |
| `check-security`| no       | `.claude/skills/check-security/SKILL.md`         |

`check-security` stays slash-only and untouched; the subagent **reads its SKILL.md as a
checklist** rather than invoking the command.

### 5. Gate

- Wait for all subagents.
- If any **blocking** check returns `FAIL`:
  - Write the draft PR body to `tmp/pr/pr-body.md` with the blocking violation flagged at the
    very top (`## BLOQUANT - CGU`).
  - **Leave the local commit(s) in place** (squashed later), do **NOT** create the PR.
  - Report the violation and stop. The user fixes, then re-triggers.
- Else continue.

### 6. Artifacts (French, undercover)

- Invoke the **`french`** skill. Write to `tmp/pr/`:
  - `commit-msg.txt` (already committed in step 2; keep for reference).
  - `pr-body.md` - French PR description: summary, what changed, and a section per check
    embedding its French report (`## Sécurité`, `## CGU`). **No** "Generated with Claude" line,
    no Claude mention.
- PR title = the commit subject (French description, ASCII conventional token). The
  `<type>(<scope>)` must follow step 2b - on app-stack PRs that should deploy, never a data scope.

### 7. Create the PR (with confirmation)

- **Rebase on `origin/main` first** (before pushing): `git fetch origin main` then
  `git rebase origin/main`. A branch behind `main` is flagged out-of-date by GitHub
  (blocks the merge) and re-runs the CI for nothing. If the rebase hits conflicts,
  stop, resolve them (or surface to the user), and only continue once clean.
- Ensure the branch is pushed: `git push -u origin <branch>` (confirm first - outward-facing).
  After a rebase that rewrote already-pushed commits, this needs `--force-with-lease`.
- Create the PR, **base `main`**, with the title + `tmp/pr/pr-body.md`, via either:
  - `mcp__github__create_pull_request` (preferred per CLAUDE.md), or
  - `gh pr create --base main --title "<titre>" -F tmp/pr/pr-body.md`.
- **Always confirm with the user before pushing and before opening the PR** (outward-facing).

## Output

Report: branch, commit subject, each check's verdict (PASS/FAIL + count), the PR URL once
created - or the blocking reason if halted at the gate - and the **release verdict** per step 2b
(will this PR cut a version and deploy, or not, and why).
