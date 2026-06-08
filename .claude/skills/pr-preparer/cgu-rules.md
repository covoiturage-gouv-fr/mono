---
name: cgu-rules
description: Cached, machine-checkable distillation of the RPC CGU - read by the pr-preparer cgu-guard check.
canonical_url: https://doc.covoiturage.beta.gouv.fr/nos-services/le-registre-de-preuve-de-covoiturage/cgu-conditions-generales-dutilisation-de-covoiturage.beta.gouv
last_synced: 2026-06-08
ttl_days: 7
---

# CGU rules - guard checklist

Cached distillation of the CGU for automated checking. **Auto-maintained**: `pr-preparer`
refreshes this file from `canonical_url` when `last_synced` is older than `ttl_days` (7).
Each rule has an id, the CGU section, a severity, and what a diff must be flagged for.

`cgu-guard` is a **blocking** check: any `blocker` FAIL halts PR creation.

<!-- TODO extend: add rules as the CGU evolves. Keep ids stable; cite the CGU section. -->

## CGU-1 - Status definitiveness after 48h  (CGU 2.1.1)  [severity: blocker]

> "the status associated with a trip by the carpool proof registry becomes definitive after
> 48h (trip completion time)."

The status of a trip becomes **final and immutable 48h after trip completion**
(`carpool_v2.carpools.end_datetime`). Classification cannot change past that window.

**Flag the diff if it:**
- mutates `carpool_v2.status` (`acquisition_status`, or `fraud_status`/`anomaly_status` where
  it changes incentive eligibility) without a window guard restricting it to trips whose
  completion is within 48h of the operation;
- adds a batch / migration / backfill / reclassification job over `carpool_v2.status` lacking a
  predicate equivalent to `end_datetime + interval '48 hours' >= <operation_time>`;
- reclassifies historical trips to `terms_violation_error` (or any non-`processed` state) in
  bulk without the 48h filter.

**Reference incident:** 2026-06-07 a batch flipped 213,498 trips `processed ->
terms_violation_error` ignoring this window; all were already definitive. This rule exists to
catch that class of change before merge.

## CGU-2 - Personal data retention limits  (CGU - données personnelles)  [severity: blocker]

Personal data is kept only for the retention period stated in the CGU; beyond it, data is
deleted or anonymised.

**Flag the diff if it:**
- extends or removes a retention / TTL / purge boundary on personal data
  (`driver_*`/`passenger_*` phone, `*_identity_key`, `*_travelpass_*`);
- disables or weakens an anonymisation / purge job;
- persists raw personal data into a store that has no retention enforcement.

## CGU-3 - PII minimisation and exposure  (CGU - données personnelles)  [severity: high]

Personal and identifying data must not be exposed beyond what the CGU permits.

**Flag the diff if it:**
- logs, returns in an API response, or writes to an export raw `*_phone`, `*_identity_key`,
  or `*_travelpass_*` where a truncated/hashed form is expected
  (`driver_phone_trunc`, `passenger_phone_trunc`, ...);
- widens the fields exposed by an export, public stat, or attestation beyond the documented set;
- removes truncation/hashing applied to identifying fields.

## How the guard reports

Return French, structured:

```
Verdict: PASS | FAIL
Pour chaque constat:
  - Règle: CGU-<n>
  - Gravité: blocker | high | ...
  - Emplacement: fichier:ligne
  - Problème: <description>
  - Recommandation: <correctif>
```

A `blocker` FAIL must halt PR creation.
