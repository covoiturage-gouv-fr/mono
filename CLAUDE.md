# CLAUDE.md

Caveman mode always active.

## Project

RPC (Registre de Preuve de Covoiturage) - beta.gouv.fr. Certifies carpooling journeys. Intermediary between operators, mobility authorities, orgs.

## Repo structure

| Dir | What | Stack |
| --- | ---- | ----- |
| `api/` | Backend | Deno 2.x, TS, Express, Inversify |
| `app-partners/` | Partner dashboard | Next.js 15, React 19, DSFR |
| `app-observatory/` | Public stats | Next.js 15, React 19, MapLibre, Deck.gl |
| `app-attestation/` | Cert generator | Angular 16 (frozen) |
| `cms/` | CMS | Strapi 4 |
| `shared/` | Shared TS types | (deprecated) |
| `dbt/` | Analytics | DBT, Python (deprecated) |
| `docker/` | Containers | Docker |

Each dir has `README.md`. Skills in `.claude/skills/`.

## Code style

TDD. Simple, testable, readable. Repeat twice then refactor. No over-engineering.

Comments: only for quirks. No narration of what the code already says.

### Tests

Unit: specific calcs, edge cases. Integration: DB repositories.

### Code reviews

Sub-agents:

- `/check-security` security review
- `/check-perf` perf analysis
- `/check-qa` Deno standards + project coherence
- `/check-doc` docs + CLAUDE.md updated with code

### Docs

Notion = internal technical docs.
`api/specs` published at <https://tech.covoiturage.beta.gouv.fr> via bump.sh CI.
README.md = entrypoints, update alongside code.

### Skills

Project-specific only: `.claude/skills/`. Never `~/.claude/skills/`.

### MCP tools

Use MCP tools when available:

- **GitHub** (`mcp__github__*`) - PRs, issues, branches, search. Prefer over `gh` CLI.
- **Notion** (`mcp__claude_ai_Notion__*`) - internal technical docs. Read before writing new docs.
- **Postgres** - query DB directly when available. Prefer over `docker exec psql`.

### Git

Never code or commit on a branch directly. Always work in a worktree + PR (skill /prep-pr); the main working dir must always stay on `main`.
Always rebase onto main before pushing PR
Claude cannot commit. Human review required before commit.
PRs are concise. Context, debrief, analysis go on Notion only — not in the PR.
