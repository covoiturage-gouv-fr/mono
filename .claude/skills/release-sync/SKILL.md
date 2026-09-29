---
name: release-sync
description: Use when merging between the long-lived branches main and next - triggers on "release-sync", "main-to-next", "next-to-main", "remets next à jour", "rattrape next", "main dans next", "livre next en prod", "passe next sur main", or after a stable release was published on main. Checks what is pending, opens the main -> next or next -> main PR and merges it with an explicit merge commit (never squash).
allowed-tools: Bash, Read, Grep, Write, AskUserQuestion
---

# Release sync (`main` <-> `next`)

Opens and merges the two PRs between long-lived branches described in `docs/GITFLOW.md`
(source of truth, read it first). Feature PRs are **not** handled here: that is `pr-prep`.

| Mode | PR | When |
| ---- | -- | ---- |
| `main-to-next` | `main` -> `next` | after **every** stable `vX.Y.Z` published from `main`, before any other merge into `next` |
| `next-to-main` | `next` -> `main` | once the `rc` running in demo is validated: ships `next` to production |

Both PRs are merged with a **merge commit**. GitHub pre-selects the last method used and the
ruleset cannot filter on the source branch, so the method is always passed explicitly.

## Hard rules

- **Never** squash or rebase these PRs, never rebase or force-push `main` / `next`, never create a
  tag by hand, never `--admin`, never self-approve.
- Remediations (squash done by mistake, re-aligning `next` on `main`): **not** automated. Point to
  the "Remédiations" section of `docs/GITFLOW.md` and stop.
- Confirm with the user before opening a PR and again before merging it (outward-facing).
- PR text in French, no mention of Claude (same convention as `pr-prep`).

## GitHub access

`mcp__github__*` tools if available, else `gh`. With neither: do the local checks, then give the
user the compare URL (`https://github.com/covoiturage-gouv-fr/mono/compare/<base>...<head>`) and
tell them to pick **"Create a merge commit"**, and stop.

| Action | MCP | `gh` |
| ------ | --- | ---- |
| open PR already there? | `list_pull_requests` (head, base, state open) | `gh pr list --head <head> --base <base>` |
| create | `create_pull_request` | `gh pr create --head <head> --base <base> --title … -F <body>` |
| checks | `get_pull_request_status` | `gh pr checks <n> --watch` |
| merge | `merge_pull_request` with `merge_method: "merge"` | `gh pr merge <n> --merge` |

## Common start

```bash
git fetch origin main next --tags
STABLE=$(git describe --tags --abbrev=0 --match 'v[0-9]*' --exclude '*-*' origin/main)
RC=$(git describe --tags --abbrev=0 --match 'v*-rc.*' origin/next 2>/dev/null)
[[ "$STABLE" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "tag stable inattendu : $STABLE"; exit 1; }
```

Always quote `"$STABLE"` / `"$RC"` in the commands below.

## Mode `main-to-next`

1. **Needed?**
   - `git merge-base --is-ancestor "$STABLE" origin/next` fails -> **required** (the next `rc` would
     fail with `EINVALIDNEXTVERSION` or take a wrong number).
   - Tag reachable but `git rev-list --count origin/next..origin/main` > 0 -> optional (commits
     without a release, e.g. `chore`/`ci`). Say so and ask.
   - Otherwise -> nothing to do, stop.
2. **Conflicts?** `git merge-tree --write-tree origin/next origin/main` (non-zero exit = conflicts).
   - Clean -> PR straight from `main` (head `main`, base `next`).
   - Conflicts -> the PR cannot be fixed on `main`. Prepare `git switch -c "sync/main-to-next-$STABLE"
     origin/next && git merge origin/main`, list the conflicted files, and **hand over to the user**
     to resolve and commit (CLAUDE.md: Claude does not commit). The PR then goes from that branch
     to `next`, still merged with a merge commit.
3. **Open PR** (reuse an existing open one). Title: `chore(release): rattrapage main dans next
   ($STABLE)`. Body: the stable tag, the number of commits brought back, and "fusion en merge
   commit, jamais en squash".
4. **Merge**: wait for the checks, confirm, merge with `merge_method: "merge"` / `--merge`. If the
   ruleset refuses (review required, missing permission): stop and tell the user to merge with
   **"Create a merge commit"**.
5. **Verify**: `git fetch origin next --tags && git merge-base --is-ancestor "$STABLE" origin/next`.

## Mode `next-to-main`

1. **Preconditions**
   - `git merge-base --is-ancestor origin/main origin/next` fails -> `next` is behind `main`: run
     `main-to-next` first.
   - `git rev-list --count --no-merges origin/main..origin/next` = 0 -> nothing to ship, stop.
2. **What goes to production** (report in French):
   - commits: `git log --no-merges --format='%h %s' origin/main..origin/next`, grouped as
     `feat` / `fix`-`perf`-`revert` / `BREAKING` / non-releasing;
   - expected version: `$RC` without its `-rc.N` suffix (explain if the commits suggest another bump);
   - **not validated in demo**: `git rev-list --no-merges "$RC"..origin/next` (commits after the last
     `rc`) -> warn, they reach production without demo;
   - migrations: `git diff --name-only origin/main...origin/next -- api/src/db/migrations`.
3. **Explicit go** from the user: this ships to **production**.
4. **Open PR** head `next`, base `main`. Title: `chore(release): livraison de next en production
   (<version attendue>)`. semantic-release reads the individual commits brought by the merge
   commit, so the title does not decide the version. Body: the report of step 2.
5. **Merge**: checks green, confirm, `merge_method: "merge"` / `--merge`. Never squash: only the PR
   title would remain and the `feat`/`fix` of the `rc` would be lost.
6. **Afterwards**: once the stable tag appears on `main` (`release` job), `main-to-next` is
   required. Offer to run it; check with `git fetch --tags` +
   `git describe --tags --abbrev=0 --match 'v[0-9]*' --exclude '*-*' origin/main`.

## Output

Mode, state found (stable tag, last `rc`, reachable or not, commits pending), PR URL, merge done
(with method) or what the user must click, and the verification result. For `next-to-main`: the
version expected in production and the commits never validated in demo.
