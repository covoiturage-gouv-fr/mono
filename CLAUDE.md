# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Registre de Preuve de Covoiturage (RPC) - A beta.gouv.fr initiative that certifies carpooling journeys to incentivize shared mobility in France. Acts as trusted intermediary between carpool operators, mobility authorities, and organizations.

## Repository Structure

Monorepo with these main components:

| Directory | Description | Tech Stack |
| --------- | ----------- | ---------- |
| `api/` | Main backend | Deno 2.x, TypeScript, Express, Inversify |
| `app-partners/` | Partner dashboard | Next.js 15, React 19, DSFR |
| `app-observatory/` | Public statistics | Next.js 15, React 19, MapLibre, Deck.gl |
| `app-attestation/` | Certificate generator | Angular 16 (frozen) |
| `cms/` | Content management | Strapi 4 |
| `shared/` | Shared TypeScript types | TypeScript (deprecated) |
| `datalake/` | Analytics transformations (active dbt project) | DBT, Python |
| `dbt/` | Analytics transformations (legacy, frozen) | DBT, Python |
| `sqlmesh/` | SQL transformations (phased out, do not extend) | SQLMesh, Python |
| `docker/` | Container configurations | Docker |

Each directory has its own `README.md`. Claude skills in `.claude/skills/` provide coding patterns for each domain.

## Coding style

Simple, testable and readable.
Repeating code twice is fine, then refactor.
Do not over-engineer solutions.

### Testing

Write unit tests for specific calculations. Target the edge cases.
Write integration tests for database repositories

### Code Reviews

Use sub-agents to perform :

- `/check-security` a security code review
- `/check-perf` a performance analysis with improvement suggestions if needed
- `/check-qa` a quality code review to enforce up to date Deno coding standards and project coherence
- `/check-doc` to make sure documentation and CLAUDE instructions are updated alongside the code

### documentation

Technical internal documentation is available on Notion.
`api/specs` are published on <https://tech.covoiturage.beta.gouv.fr> using bump.sh in a Github Action.
Local README.md files are used as entrypoints to the different applications and must be updated alongside code.
`datalake/README.md` is the primary reference for the active dbt project. `sqlmesh/README.md` covers the legacy SQLMesh models that are being phased out — do not extend them; port to `datalake/` instead.

### Skills

Skills must be project-specific (`.claude/skills/`), never cross-project (`~/.claude/skills/`).

### Git

CLAUDE agent cannot commit. Code must be human reviewed before committing to the repository.
