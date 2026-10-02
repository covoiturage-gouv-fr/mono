---
name: pr-bot
description: Use when processing the open Dependabot PRs - triggers on "pr-bot", "traite les PR dependabot", "merge les PR dependabot", "fournée dependabot", "mises à jour de dépendances en attente". Gathers the selected Dependabot PRs on a dedicated branch, analyses and merges them there, runs the CI on the grouped PR, merges it into main, then brings main back into next with a merge commit.
allowed-tools: Bash, Read, Grep, Write, AskUserQuestion, Skill
---

# PR bot (Dependabot)

One batch = one branch `deps/dependabot-<YYYY-MM-DD>` cut from `origin/main`. The Dependabot PRs
are retargeted onto it, merged there one by one, then the branch goes to `main` through a single
PR. Config: `.github/dependabot.yml`. Gitflow: `docs/GITFLOW.md` (source of truth).

```text
dependabot/* ──squash──▶ deps/dependabot-<date> ──squash──▶ main ──merge commit──▶ next
```

## Hard rules

- Only PRs authored by `app/dependabot` with a `dependabot/*` head. Never touch other PRs.
- `gh` always through `pass-env gh …` (never bare `gh`).
- Confirm with the user: the PR selection (step 2) and the merge into `main` (step 6). Merging
  into `main` publishes a stable release to **production**.
- Never `--admin`, never self-approve, never force-push `main` / `next`, never tag by hand.
- Commits (conflict resolution only): signed `git commit -S`, the user types the PIN and touches
  the key. A timeout = user AFK: wait for their go, do not retry in a loop.
- PR text in French, concise, no mention of Claude, no attribution lines.

## 0. Inventory

```bash
git fetch origin main next
pass-env gh pr list --author app/dependabot --state open --limit 100 \
  --json number,title,baseRefName,headRefName,mergeable,labels,url
```

A Dependabot PR already based on a `deps/dependabot-*` branch = a batch in progress: resume it
instead of starting a new one. Nothing open -> stop.

## 1. Analysis

Per PR, `pass-env gh pr view <n> --json title,body,files,labels` and
`pass-env gh pr diff <n> --name-only`:

| Check | Red flag |
| ----- | -------- |
| bump type (title / body) | **major** -> read the changelog in the body, look for breaking changes |
| files | anything other than manifest + lockfile (`package*.json`, `pyproject.toml`, `uv.lock`, `Dockerfile`, `compose*.yml`, workflows) -> suspicious, do not merge |
| security | `security` label or advisory in the body -> priority |
| directory | `app-attestation/` (frozen, Angular 16): security fixes only; `dbt/` (deprecated, absent from `dependabot.yml`): skip unless critical advisory |
| overlap | two PRs bumping the same package in the same dir (e.g. a single bump + a group) -> keep the superset, close the other |
| CI coverage | `quality.yml` covers `api/`, `app-partners/`, `app-observatory/`, `shared/`, `docker/api/`; `quality-datalake*.yml` cover `datalake/` and `api-datalake/` -> other dirs have no CI, say so |

For a major on a covered app, grep the usage of the package in the code
(`git grep -n "<package>" -- <dir>`) and read the migration guide if one is linked.

## 2. Selection

Present a table (in French): `#`, title, dir, bump type, security, verdict (`merge` / `skip` /
`close`) with a one-line reason. Ask the user to validate (`AskUserQuestion`, multiSelect on the
PRs to merge). `skip` PRs stay untouched on `main`. `close` PRs:
`pass-env gh pr close <n> --comment "<reason>"` (Dependabot will not reopen that version).

## 3. Batch branch

Created remotely, the main working dir stays on `main`:

```bash
BATCH="deps/dependabot-$(date +%F)"
git push origin "origin/main:refs/heads/$BATCH"
```

`publickey` refused -> fallback in the `git-commit-push-workflow` memory (gh credential helper).

## 4. Retarget and merge

Order: security first, then by directory, groups before single bumps. For each selected PR:

```bash
pass-env gh pr edit <n> --base "$BATCH"
pass-env gh pr update-branch <n>          # merges $BATCH into the PR -> CI on the combined code
pass-env gh pr checks <n> --watch
pass-env gh pr merge <n> --squash
```

- The quality workflows trigger on PRs to `deps/**`. A base change alone does not run them:
  `update-branch` pushes a commit and does. "Already up to date" -> the checks already on the
  head commit (run against `main`) stand.
- Red CI -> do not merge, report the failing job (`pass-env gh run view <run> --log-failed`),
  put the PR back on `main` (`pass-env gh pr edit <n> --base main`) and go on.
- `update-branch` fails on a conflict (lockfile touched by a PR merged before) -> resolve locally
  in a worktree (`superpowers:using-git-worktrees`) on the Dependabot head:
  `git merge origin/$BATCH`, regenerate the lockfile with the tool of the dir
  (`npm install --package-lock-only`, `uv lock`), signed commit, push to the Dependabot head, then
  CI and merge as above.

## 5. Batch PR and CI

Title always `fix(deps): mises à jour Dependabot du <date>`, no confirmation: it becomes the
squash commit on `main` and triggers the release and the deployments (the `release` job only runs
when the diff touches the app stack, see `docs/GITFLOW.md`).

```bash
pass-env gh pr create --base main --head "$BATCH" --title "<title>" -F <body file>
pass-env gh pr checks <n> --watch
```

Body: one line of context + the list of merged PRs (`- #n titre`) + the closed / skipped ones.

`deps/**` is unprotected: before merging, check that `pass-env gh pr diff <n> --name-only` only
lists manifests / lockfiles and that `git log origin/main..origin/$BATCH` only holds the squashes
of the merged PRs (+ the signed conflict resolutions). Anything else -> stop and report.

Red CI (rare, each PR was green on the batch): find the guilty PR from the failing job and its dir
(`pass-env gh run view <run> --log-failed`), revert its squash commit on `$BATCH`
(`git revert -S <sha>`, signed) and report it. Never merge with a red required check.

## 6. Merge into `main`

Green CI + user go -> `pass-env gh pr merge <n> --squash --delete-branch`. Ruleset refuses
(review required) -> give the PR URL, the user merges in **squash**, wait for it.

## 7. `main` -> `next`

Run the `release-sync` skill, mode `main-to-next` (merge commit, never squash). Wait first for the
`release` job on `main`; a new stable tag makes the sync required
(`git fetch --tags && git describe --tags --abbrev=0 --match 'v[0-9]*' --exclude '*-*' origin/main`).
No tag (data-only batch): release-sync sees the sync as optional, do it anyway.

## Output

Batch branch, merged / skipped / closed PRs (numbers), batch PR URL, CI result, merge into `main`
(method, stable tag if any), `main` -> `next` PR URL and merge.
