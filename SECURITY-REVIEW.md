# Security review: the find-and-fix loop

This repo runs an AI-assisted, **static-only** security review workflow adapted
from Anthropic's
[*Using LLMs to Secure Source Code*](https://claude.com/blog/using-llms-to-secure-source-code)
and the companion
[`defending-code-reference-harness`](https://github.com/anthropics/defending-code-reference-harness).

It is a deeper complement to `/check-security`, not a replacement.

| Tool | When | Scope |
|---|---|---|
| `/check-security` | On a PR, fast | the git diff |
| `/threat-model` -> `/vuln-scan` -> `/triage` -> `/patch` | Periodically, or when the attack surface changes meaningfully | a whole subtree (currently `api/`) |

## The four steps

1. **`/threat-model`** — map assets, entry points, trust boundaries, and the
   threats that matter. Output: `THREAT_MODEL.md` (repo root, committed). It is
   the map that tells `/vuln-scan` where to look and `/triage` which findings
   matter. Modes: `interview` (owner present), `bootstrap` (derive from code +
   past vulns), `bootstrap-then-interview` (both).

2. **`/vuln-scan`** — whole-tree static review. Partitions the target into
   focus areas (seeded from `THREAT_MODEL.md`) and fans out parallel review
   subagents. Output: `VULN-FINDINGS.{json,md}` — candidates, not verdicts.

3. **`/triage`** — adversarial verification. N independent skeptical verifiers
   re-derive each finding from the code and vote; duplicates collapse; survivors
   are re-ranked by precondition-derived exploitability and tagged with an
   owner. Output: `TRIAGE.{json,md}`. This is where false positives are
   removed.

4. **`/patch`** — candidate fixes for confirmed findings. A per-finding patch
   author plus an independent reviewer (which never sees the finding prose, to
   block prompt injection via scanned source). Output: inert diffs under
   `PATCHES/` with an embedded regression test. **Never applied automatically.**

## Usage (against `api/`)

```sh
# 1. Build / refresh the threat model (committed at repo root)
/threat-model bootstrap api/            # or: interview / bootstrap-then-interview

# 2. Scan
/vuln-scan api/

# 3. Verify, dedupe, rank
/triage api/VULN-FINDINGS.json --repo api/

# 4. Draft fixes for the top findings
/patch TRIAGE.json --repo api/ --top 5
```

## Safety properties

- **Static only.** No step builds, runs, fuzzes, or sends requests against the
  target. No network access to the target's infrastructure.
- **No execution oracle.** The reference harness's autonomous sandboxed
  pipeline (gVisor + ASAN fuzzing, built for C/C++ memory safety) was
  intentionally **omitted** — it does not fit the Deno/TS/SQL/Next.js stack.
  Where execution verification would help, the skills recommend a human build a
  proof-of-concept in a controlled environment.
- **`/patch` never applies a diff.** There is no `--apply` flag by design, so it
  cannot be prompt-injected into modifying the repo. A human reviews and applies
  (per project policy, the CLAUDE agent cannot commit).
- **Checkpoint state and scan artifacts are git-ignored.** `THREAT_MODEL.md` is
  committed; `VULN-FINDINGS.*`, `TRIAGE.*`, `PATCHES/`, and the `.*-state/`
  scratch dirs are not (they are ephemeral and may contain sensitive detail).

## Tuning the skills to RPC

- `/vuln-scan`'s category list is aligned with
  `.claude/skills/check-security/SKILL.md` and the stack: SQL injection (raw
  interpolation only — the ILOS `sql` tagged-template is safe), cross-tenant
  IDOR (missing `operator_id` scope), JWT/operator-token validation, PII
  exposure (`driver_identity_key`, identities, phone hashes), SSRF,
  deserialization, prototype pollution.
- `/triage`'s 16 false-positive exclusion rules already cover framework
  auto-escaping, parameterized queries, volumetric DoS (infra layer),
  test/migration code, and outdated deps (handled by `npm`/`deno audit` + CI).
  Add org precedents with `--fp-rules <file>`.
- Secrets already whitelisted in `.talismanrc` are out of scope for findings.

The skills and their prompts are the reusable part. Re-run periodically; expect
diminishing returns after the first deep pass, and budget time for triage, not
just scanning.
