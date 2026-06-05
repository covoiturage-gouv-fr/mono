---
name: vuln-scan
description: >-
  Static source-code vulnerability scan for the RPC stack (Deno/TS api, Next.js
  frontends, SQL). Reads a target directory (and THREAT_MODEL.md if present),
  spawns parallel review subagents per focus area, and writes VULN-FINDINGS.json
  + .md for /triage to consume. Read-only — no building, running, or network.
  Use when asked to "scan for vulns", "review this code for security issues",
  "find bugs in <dir>", or as the step between /threat-model and /triage. For a
  quick PR-diff review use /check-security instead.
argument-hint: "<target-dir> [--focus <area>] [--single] [--extra <file>] [--no-score]"
allowed-tools:
  - Read
  - Glob
  - Grep
  - Write
  - Task
  - Bash(rg:*)
  - Bash(grep:*)
  - Bash(ls:*)
  - Bash(wc:*)
  - Bash(head:*)
  - Bash(file:*)
---

# /vuln-scan

Static vulnerability review of a source tree. Produces `VULN-FINDINGS.json`
(+ a human-readable `.md`) that `/triage` ingests directly. Second leg of the
find-and-fix loop (`/threat-model -> /vuln-scan -> /triage -> /patch`); see
`SECURITY-REVIEW.md`.

**This skill does not execute code.** It reads source and reasons about it.
For execution-verified findings, recommend a human build a proof-of-concept in
a controlled environment. There is no autonomous fuzzing pipeline in this repo.

**Relationship to `/check-security`.** `/check-security` is the fast,
diff-scoped review run on a PR. `/vuln-scan` is the deep, whole-tree pass that
fans out parallel reviewers per focus area and feeds `/triage`. They share the
same notion of "what is a finding"; keep the category list below in sync with
`.claude/skills/check-security/SKILL.md`.

**Tool fallbacks.** Prefer the dedicated Glob and Grep tools. Some sessions
do not provision them — `allowed-tools` is a permission filter, not a loader,
so listing them here does not make them appear. When Glob/Grep are
unavailable, fall back to the read-only Bash commands whitelisted above:
`rg --files <scope>` / `ls -R` for enumeration, `rg -n` / `grep -rn` for
search, `wc` / `head` / `file` for sniffing. These are the ONLY permitted
Bash commands; do not write helper scripts or pipe target content into a
shell interpreter.

## Arguments

- `<target-dir>` (required) — directory to scan (e.g. `api/`). Relative or absolute.
- `--focus <area>` — scan only this focus area (repeatable). Skips recon.
- `--single` — no subagent fan-out; one sequential pass. Use on tiny targets
  or when debugging the prompt.
- `--extra <file>` — append the contents of `<file>` to the review brief
  (after the category list). Use to add org-specific vulnerability classes,
  compliance checks, or stack-specific patterns. Plain text; same shape as
  the category blocks below.
- `--no-score` — skip the Step 3b confidence pass (saves a round of
  subagents). Findings keep the scanner's self-reported confidence only.

## Step 1 — Scope

1. Resolve `<target-dir>`. If it doesn't exist or has no source files, stop
   with an error.
2. Look for `THREAT_MODEL.md` (repo root for an `api/` scope, or
   `<target-dir>/THREAT_MODEL.md`). If present, parse its section 3 "Entry
   points & trust boundaries" table and section 4 "Threats" table for focus
   areas and threat classes. This is the preferred scoping input. The section 6
   "Open questions" are high-value first focus areas.
3. If no THREAT_MODEL.md and no `--focus`: do a **quick recon** — list the
   source tree, read entry points and dispatch code (ILOS `@handler` actions,
   repositories, middleware), and propose 3-10 focus areas using the pattern
   `<subsystem> (<function/file>) — <key operations>`.
4. If `--focus` was given, use exactly those.

Tell the user the focus areas you'll scan and the source-file count before
fanning out.

## Step 2 — Fan out

Unless `--single`, spawn **one Task subagent per focus area** in parallel.
Cap at 10 concurrent. Each subagent gets the review brief below with its
focus area filled in. On tiny targets (<15 source files), fall through to
`--single` automatically.

### Review brief (per subagent)

```
You are conducting authorized static security review of source code. Your
focus area: **{focus_area}**. Other agents cover other areas; duplication
is wasted effort.

TARGET: {target_dir}
TRUST BOUNDARY: {from THREAT_MODEL.md section 3, or "authenticated-untrusted operator / unauth network → application logic & data"}

TASK: read the source in your focus area and identify candidate
vulnerabilities. This is static review — do NOT build, run, or probe
anything. Reason from the code.

REPORTING BAR: report anything with a plausible exploit path. Skip style
concerns, best-practice gaps, and purely theoretical issues with no attack
story at all — but if you're unsure whether something is real, REPORT IT
with a low confidence score rather than dropping it. A downstream triage
step does the rigorous verification; your job is to not miss things.

WHAT TO LOOK FOR (RPC stack: Deno/TypeScript, Next.js, PostgreSQL/SQL):

  INJECTION & CODE EXECUTION — HIGH VALUE:
  - SQL injection: raw string interpolation into queries. The ILOS `sql`
    tagged-template (and parameterized `$1` placeholders) is SAFE; the signal
    is concatenation or interpolation that bypasses it — e.g.
    `client.query(`... ${userInput} ...`)`, string-built WHERE clauses,
    dynamic identifiers/ORDER BY from untrusted input.
  - command injection (Deno.Command / subprocess with untrusted args)
  - path traversal in file operations (untrusted path joined to fs read/write)
  - unsafe deserialization: YAML loaders on untrusted input, JSON.parse feeding
    a dangerous sink, prototype pollution via merged untrusted objects
  - template / expression injection; eval / dynamic import on untrusted input
  - SSRF: untrusted host/URL passed to fetch/http client (host+protocol
    attacker-controlled, not just the path)

  AUTH, AUTHZ, MULTI-TENANCY — HIGH VALUE:
  - authentication or authorization bypass, privilege escalation to admin
  - missing operator/tenant scoping → IDOR / cross-tenant read or write
    (an operator reaching another operator's journeys, proofs, identities)
  - JWT / operator-token validation flaws (alg confusion, missing signature
    or expiry check, scope not enforced)
  - middleware that can be skipped on some routes; TOCTOU on a security check

  DATA EXPOSURE & PII (RGPD) — HIGH VALUE:
  - PII (driver/passenger identities, phone hashes, driver_identity_key) in
    logs, error responses, or over-broad API responses
  - secrets / credentials hardcoded or leaked in errors
  - sensitive data returned without the requesting operator's scope applied

  DENO / PLATFORM — note briefly:
  - over-broad `--allow-*` permissions (defence-in-depth)
  - weak crypto, broken cert validation

  MEMORY SAFETY — only relevant inside Deno FFI / unsafe / WASM glue, if any.

DO NOT REPORT (common false positives — skip even if technically present):
  - volumetric DoS / rate-limiting / resource-exhaustion — BUT unbounded
    recursion, algorithmic-complexity blowup, or ReDoS driven by untrusted
    input ARE reportable
  - memory-safety findings in memory-safe languages (TS/JS) outside FFI/WASM
  - XSS in React/Next/Angular unless via dangerouslySetInnerHTML,
    bypassSecurityTrustHtml, v-html, or equivalent raw-HTML escape hatch
  - SQL "injection" through the ILOS `sql` template or parameterized queries
    where values are bound, not concatenated
  - findings in test files, fixtures, migrations seed data, build scripts,
    docs, or .ipynb
  - missing hardening / best-practice gaps with no concrete exploit
  - env vars and CLI flags as the attack vector (operator-controlled)
  - regex injection, log spoofing, open redirect, missing audit logs
  - outdated third-party dependency versions (handled by npm/deno audit + CI)
  - secrets already whitelisted in .talismanrc

{if --extra <file> was given: append its contents here verbatim}

For each finding you DO report, trace: where does the untrusted input
enter, what path reaches the sink, and what condition triggers it.

OUTPUT — one block per finding, nothing else:

<finding>
<id>F-{focus_idx:02d}-{n:02d}</id>
<file>{relative/path}</file>
<line>{line_number}</line>
<category>{sql-injection | command-injection | path-traversal | ssrf | deserialization | prototype-pollution | auth-bypass | idor-cross-tenant | jwt-validation | pii-exposure | hardcoded-secret | xss | ...}</category>
<severity>{HIGH | MEDIUM | LOW}</severity>
<confidence>{0.0-1.0}</confidence>
<title>{one line}</title>
<description>{root cause, attacker control, trigger condition, data flow from entry to sink. Cite line numbers.}</description>
<exploit_scenario>{concrete attack: what input, from where, causing what outcome}</exploit_scenario>
<recommendation>{specific fix: parameterize the query, add operator-scope filter, validate the token scope, etc.}</recommendation>
</finding>

SEVERITY: HIGH = directly exploitable → RCE, data breach, auth bypass,
cross-tenant access, mass PII exposure. MEDIUM = significant impact under
specific conditions. LOW = defense-in-depth.

If you find nothing reportable in your area after a thorough read, emit a
single <finding> with category=none and a one-line note of what you covered.
```

## Step 3 — Collate

1. Collect `<finding>` blocks from all subagents. Drop `category=none`
   placeholders.
2. **Light dedupe** — if two findings cite the same `file:line` with the
   same category, keep the one with the longer description and note the
   duplicate id. (Heavy dedupe is `/triage`'s job; don't over-engineer here.)
3. Assign stable ids `F-001`, `F-002`, ... in (severity desc, file, line)
   order.

## Step 3b — Confidence pass (skip if `--no-score`)

A cheap second-opinion read that **ranks** findings by signal quality.
**Nothing is dropped** — this pass calibrates `confidence` so humans and
`/triage` see high-signal findings first. Spawn **one Task subagent per
finding** in parallel with the brief below. Shallow: re-read and score, not
a full reachability trace.

### Scoring brief (per finding)

```
You are giving ONE candidate security finding an independent confidence
score. You are NOT deciding whether to keep it — every finding is kept.
You are deciding how likely it is to survive rigorous triage.

FINDING:
{the full <finding> block}

TARGET: {target_dir} (you may Read/Grep inside it; do NOT execute)

STEP 1 — Re-read the cited code. Open {file} around line {line}. Does the
code actually do what the description claims?

STEP 2 — Check against common false-positive patterns (volumetric DoS,
memory-safe language, test/fixture/migration/doc file, framework auto-escape,
ILOS sql/parameterized query, env-var vector, missing-hardening-only,
regex/log injection, outdated dep, .talismanrc-whitelisted secret). A match
lowers confidence sharply but does not auto-zero it.

STEP 3 — Score 1-10 that this is a real, actionable vulnerability:
  1-3  likely false positive or noise
  4-5  plausible but speculative
  6-7  credible, needs investigation
  8-10 high confidence, clear pattern

OUTPUT (exactly this, nothing else):
  CONFIDENCE: <1-10>
  REASON: <one line>
```

**Resolve:** overwrite each finding's `confidence` with the score
(normalized to 0.0-1.0) and attach `confidence_reason`. Re-sort findings
by (`confidence` desc, `severity` desc, `file`, `line`) and reassign ids
`F-001..` in that order. Compute `low_confidence_count` = findings with
confidence < 0.4, for the summary line.

## Step 4 — Write output

Write **both** files to `<target-dir>/`:

**`VULN-FINDINGS.json`** — the `/triage` ingest shape:

```json
{
  "target": "<target-dir>",
  "scanned_at": "<iso8601>",
  "focus_areas": ["..."],
  "findings": [
    {
      "id": "F-001",
      "file": "relative/path.ts",
      "line": 123,
      "category": "idor-cross-tenant",
      "severity": "HIGH",
      "confidence": 0.9,
      "title": "...",
      "description": "...",
      "exploit_scenario": "...",
      "recommendation": "...",
      "confidence_reason": "..."
    }
  ],
  "summary": {"total": 0, "high": 0, "medium": 0, "low": 0, "low_confidence": 0}
}
```

Findings are sorted by `confidence` desc (then severity, file, line), so
the top of the file is the highest-signal material.

**`VULN-FINDINGS.md`** — human-readable: a summary table (id | severity |
category | file:line | title), then one `### F-NNN` section per finding with
the full description.

## Step 5 — Hand back

Tell the user:

1. Counts: N findings (H/M/L split, X low-confidence), across K focus
   areas, from M source files.
2. Top 3 by confidence, one line each.
3. Next step: `> /triage <target-dir>/VULN-FINDINGS.json --repo <target-dir>`
4. Remind: these are **static candidates**, not verified. For
   execution-verified confirmation, recommend a human-built PoC in a
   controlled environment.

## Constraints

- **Never execute target code.** No Bash beyond the read-only whitelist, no
  builds, no `docker`, no network. If the user asks you to "reproduce" or
  "confirm with a PoC," decline and recommend a human-built PoC.
- **Don't fabricate line numbers.** Every `file:line` you emit must be
  something you Read or Grep'd. If unsure of the exact line, cite the
  function and say so in the description.
- **Stay in `<target-dir>`.** Don't follow symlinks or `..` out of it.
- Findings are candidates for `/triage`, not final verdicts. **This skill
  never drops a finding** — Step 3b only ranks. `/triage` does the rigorous
  N-vote verification and is where false positives actually get removed.

## Provenance

Adapted from `anthropics/defending-code-reference-harness` (`vuln-scan` skill)
and `anthropics/claude-code-security-review` (`/security-review`), retuned from
C/C++ memory-safety to the RPC Deno/TS/SQL/web stack. The category menu is
aligned with this repo's `.claude/skills/check-security/SKILL.md`.
