# Threat Model: RPC `api/` backend

> Scope: the `api/` backend service. Built via stakeholder interview (the
> four-question framework). Maintained alongside the `/threat-model`,
> `/vuln-scan`, `/triage`, and `/patch` skills - see `SECURITY-REVIEW.md`.
> Update when the attack surface changes.

## 1. System context

The RPC `api/` backend (Deno 2.x / TypeScript, ILOS framework on Express +
Inversify) is the internet-facing, multi-tenant core of the Registre de Preuve
de Covoiturage. Carpool **operators** authenticate and submit journey data
("proofs"); the backend certifies journeys, computes mobility incentives
(subventions) for AOMs (autorites organisatrices de la mobilite) and the state,
and exposes data to a partner dashboard. It holds personal data of drivers and
passengers and is governed by **RGPD/GDPR** and **French public-sector (ANSSI /
beta.gouv.fr)** obligations. Operators are **authenticated but untrusted**:
their submitted payloads are treated as hostile input.

## 2. Assets

| asset | description | sensitivity |
|---|---|---|
| A1 operator credentials & tokens | JWT / operator API tokens authenticating data submission and partner login | critical |
| A2 personal identity data (PII) | driver/passenger identities, phone hashes, `driver_identity_key` | critical |
| A3 carpool proof integrity | certified journeys backing subsidy payments | high |
| A4 incentive/subsidy computation | policy engine computing amounts per journey | high |
| A5 per-operator data isolation | tenant boundary keeping operators' data separate | critical |

## 3. Entry points & trust boundaries

| entry_point | description | trust_boundary | reachable_assets |
|---|---|---|---|
| Operator journey ingestion API | authenticated REST endpoint accepting journey/proof payloads | authenticated-untrusted operator -> proof store | A2, A3, A5 |
| Partner dashboard auth/API | login + data read for operators/partners | unauth network -> authenticated session | A1, A2, A5 |
| Token issuance/management | operator token creation, rotation, scoping | authenticated -> credential store | A1 |
| Admin / operator-management | privileged operations on operators and data | authenticated admin -> all assets | A1, A2, A3, A4, A5 |
| Public/observatory read API | aggregated statistics | unauth network -> aggregated data | (low; mostly deprioritised) |

## 4. Threats

Sorted by (impact, likelihood) descending. `evidence` raises likelihood; it is
not the threat. Likelihood is the residual after the listed controls.

| id | threat | actor | surface | asset | impact | likelihood | status | controls | evidence |
|---|---|---|---|---|---|---|---|---|---|
| T1 | Mass PII exfiltration of driver/passenger identities | remote_auth | Partner dashboard / journey read | A2 | critical | possible | partially_mitigated | auth middleware, tenant scoping, schema validation | |
| T2 | Cross-tenant data leakage / IDOR (operator A reads or modifies operator B) | remote_auth | Operator journey ingestion, journey/proof read | A5, A2, A3 | critical | possible | partially_mitigated | tenant scoping in queries | |
| T3 | Operator token theft / impersonation | remote_unauth | Token issuance/management, auth | A1 | critical | possible | partially_mitigated | JWT auth, secrets mgmt (Talisman), reverse proxy | |
| T5 | SQL injection via operator-controlled payloads | remote_auth | Operator journey ingestion, any raw query | A2, A3, A5 | critical | rare | partially_mitigated | ILOS `sql` parameterized templates, schema validation | |
| T6 | AuthN/AuthZ bypass or privilege escalation to admin | remote_unauth | Partner dashboard auth, Admin endpoints | A1, A2, A3, A4, A5 | critical | rare | partially_mitigated | Inversify middleware auth | |
| T4 | Incentive fraud via fabricated, replayed, or inflated proofs | remote_auth | Operator journey ingestion | A3, A4 | high | likely | partially_mitigated | schema validation, business rules | |
| T7 | RGPD breach: PII over-retention, failed right-to-erasure, or re-identification | insider | Token issuance/management, identity store over time | A2 | high | possible | partially_mitigated | retention process (verify) | |
| T9 | Unsafe deserialization or path traversal via payload parsing | remote_auth | Operator journey ingestion | A2 (and process integrity) | high | rare | partially_mitigated | schema validation | |
| T8 | PII leakage via logs or verbose error responses | remote_auth | Partner dashboard / journey read, Admin endpoints | A2 | medium | possible | partially_mitigated | | |

## 5. Deprioritized

| threat | reason |
|---|---|
| Volumetric DoS / request flooding | handled at infra layer (Traefik / reverse proxy + rate limiting), not app code |
| Strapi CMS vulnerabilities | separate component (`cms/`), outside the `api/` scope of this model |
| Observatory public statistics exposure | aggregated, non-PII public data; low sensitivity |
| Client-side XSS in React/Next auto-escaped paths | framework auto-escaping; only raw-HTML escape hatches stay in scope |

## 6. Open questions

These seed the first `/vuln-scan api/` focus areas. Each is an `[Owner-states]`
claim or a control that must be confirmed in code.

- Are all operator-data queries enforced through a central tenant-scoping layer,
  or scoped per-query (so one missed `operator_id` filter = cross-tenant leak)?
  Affects: T1, T2. Verify by: trace repository query builders for a shared
  tenant guard.
- Where are operator tokens stored, and how are they scoped, rotated, and
  revoked? Affects: T3. Verify by: read token issuance + middleware validation.
- Is there an enforced PII retention / erasure job, plus a re-identification
  risk assessment for `driver_identity_key` and phone hashes? Affects: T7.
  Verify by: locate retention/anonymisation jobs and their schedule.
- Any raw SQL string interpolation that bypasses the ILOS `sql` tagged-template?
  Affects: T5. Verify by: grep for query concatenation / `client.query` with
  interpolated values.
- Does any error-handling or logging path serialise PII into responses or logs?
  Affects: T8. Verify by: review error middleware and logger calls on PII-
  bearing objects.

## 7. Provenance

- mode: interview
- date: 2026-06-05
- target: `api/` @ current `main`
- inputs: stakeholder interview (four-question framework); no `--vulns`
- owner: Jonathan Fallon

## 8. Recommended mitigations

One row per class-level control (not a per-finding patch).

| mitigation | threat_ids | closes_class | effort |
|---|---|---|---|
| Central tenant-scope guard enforced on every operator-data query | T1, T2 | partial | M |
| Short-lived scoped tokens with rotation + server-side revocation | T3 | partial | M |
| Parameterized queries only; lint/ban raw SQL string interpolation | T5 | yes | S |
| Server-side proof plausibility + anti-replay checks | T4 | partial | L |
| PII-aware logging filter + error-response scrubbing | T1, T8 | partial | S |
| Enforced retention + erasure job; document re-identification controls | T7 | partial | M |
| Authorization tests on every admin / privileged route | T6 | partial | M |
