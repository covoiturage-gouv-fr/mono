# Design — Enrichissement du payload de refus de trajet (compteurs de règle)

**Date** : 2026-07-29
**Ticket** : GEN-647 (suite)
**Statut** : design validé, à implémenter

## Contexte & objectif

Quand un trajet est refusé pour non-respect des CGU, l'opérateur reçoit une erreur JSON-RPC
(`code -32422`, HTTP 422) dont le corps `data` vaut aujourd'hui :

```jsonc
data: { terms_violation_labels: ["too_many_trips_by_day"] }
```

L'opérateur ne connaît que **le motif**, jamais **les chiffres** qui l'ont déclenché. Objectif :
préciser la cause en exposant, pour `too_many_trips_by_day`, le nombre de trajets du conducteur
et du passager sur la journée, plus la limite.

Chaîne actuelle : `CarpoolAcquisitionService.verifyTermsViolation` (retourne `string[]`)
→ `registerRequest` → `CreateJourneyAction.validateResults` → `UnprocessableRequestException(data)`.

Synergie : le fix GEN-647 (même branche `gen647-too-many-trips-fix`) calcule **déjà** `driverCount`
et `passengerCount` pour la règle `too_many_trips_by_day`. Cet enrichissement les expose — coût nul.

## Décisions

| Point | Choix |
|---|---|
| Périmètre | Générique extensible : garder `terms_violation_labels` (compat), AJOUTER un tableau d'objets |
| Nom du champ | **`terms_violation_error_details`** (convention maison `*_error_details`, cf. `anomaly_error_details` / `fraud_error_details`) — évite la collision avec le `terms_violation_details` existant de la réponse GET statut |
| Forme d'un item | `{ label, metas? }`, calqué sur `AnomalyErrorDetails` (`{ label, metas }`) |
| Entrées | **Une entrée par règle déclenchée** ; `metas` présent seulement pour `too_many_trips_by_day` pour l'instant |
| Compteurs | `metas: { driver, passenger, limit }` — `driver`/`passenger` = **valeur brute que la règle compare à `limit`** (exclut le trajet courant + canceled/expired/refusés, comme `countJourneyBy`), donc `driver > limit` ou `passenger > limit` explique le refus exactement |
| Persistance | **Aucune** en base transactionnelle. Suivi de l'indicateur = modèle datalake a posteriori (chantier distinct, hors de ce spec) |
| Séquencement | **Même PR que le fix GEN-647** |
| OpenAPI | Cible la spec **v3.4** (`api/specs/api-v3.4.yaml`), non encore publiée auprès des opérateurs |

## Forme du payload

Réponse 422 enrichie (exemple avec deux motifs) :

```jsonc
data: {
  terms_violation_labels: ["too_many_trips_by_day", "distance_too_short"],   // inchangé (compat)
  terms_violation_error_details: [
    { label: "too_many_trips_by_day",
      metas: { driver: 5, passenger: 2, limit: 4 } },
    { label: "distance_too_short" }                                          // pas de metas (pour l'instant)
  ]
}
```

## Plumbing

Source de vérité unique : `verifyTermsViolation` ne renvoie plus `string[]` mais
`TermsViolationErrorDetail[]` ; les labels s'en déduisent par `.map(d => d.label)`.
**Zéro changement de schéma DB** : on continue à stocker les labels `string[]` comme aujourd'hui.

### Types — `api/src/pdc/providers/carpool/interfaces/common.ts`

```ts
export const MAX_TRIPS_PER_DAY = 4;

export type TermsViolationLabel =
  | "distance_too_short"
  | "too_many_trips_by_day"
  | "too_close_trips"
  | "expired";

export type TermsViolationErrorDetail =
  | { label: "too_many_trips_by_day"; metas: { driver: number; passenger: number; limit: number } }
  | { label: "distance_too_short" | "too_close_trips" | "expired" };

export type TermsViolationErrorDetails = Array<TermsViolationErrorDetail>;
```

`TermsViolationErrorLabels = Array<string>` (existant) reste, dérivé des labels.

### Changements

| Fichier | Changement |
|---|---|
| `interfaces/common.ts` | + `MAX_TRIPS_PER_DAY`, `TermsViolationLabel`, `TermsViolationErrorDetail(s)` |
| `providers/CarpoolAcquisitionService.ts` (`verifyTermsViolation`) | retourne `TermsViolationErrorDetail[]` ; pousse des objets structurés au lieu de strings ; `too_many_trips_by_day` réutilise `driverCount`/`passengerCount` du fix et pose `metas: { driver, passenger, limit: MAX_TRIPS_PER_DAY }` |
| `providers/CarpoolAcquisitionService.ts` (`registerRequest`) | `const details = await verifyTermsViolation(...)` ; `const labels = details.map(d => d.label)` → statut + `setTermsViolationErrorLabels(labels)` **inchangés** ; renvoie aussi `details` |
| `interfaces/acquisition.ts` (`RegisterResponse`) | + `terms_violation_error_details: TermsViolationErrorDetails` |
| `actions/CreateJourneyAction.ts` (`validateResults`) | `throw new UnprocessableRequestException({ terms_violation_labels: labels, terms_violation_error_details: details })` |
| `api/specs/api-v3.4.yaml` | nouveau schéma `terms_violation_error_details` (items `{ label: terms_violation_label, metas }`, calqué sur `anomaly_error_details`) ; l'ajouter dans la réponse `"422"` du `POST /journeys` à côté de `terms_violation_labels` |

Le `too_close_trips` (règle voisine, `identity_key_or: false`) n'est pas modifié.

## Compatibilité

- Ajout **additif** dans `data` (free-form, `data?: any`) → non-breaking pour l'API live ; les opérateurs
  qui ignorent le nouveau champ ne cassent pas. `terms_violation_labels` est conservé tel quel.
- OpenAPI : la modification vise la v3.4 non publiée → aucune rupture de contrat déjà annoncé.

## Hors-scope (chantiers distincts)

- **Persistance des compteurs** en base : écartée (YAGNI ; recalculables).
- **Modèle datalake** de suivi de l'indicateur « % de refus `too_many` où `max(driver, passenger) ≤ 4` » :
  agrégation dbt a posteriori + carte Metabase. À concevoir séparément.
- **Réponse GET statut** (`CarpoolStatusService` → `terms_violation_details: string[]`) : laissée
  telle quelle (label-only). L'aligner sur `{ label, metas }` supposerait de persister les compteurs
  → hors-scope. Asymétrie assumée : le 422 (temps réel) porte les `metas`, le GET statut non.
- **Rétro-action** des trajets déjà refusés : script séparé (sous-page Notion GEN-647).

## Tests

- Unit/intégration `CarpoolAcquisitionService` : `too_many_trips_by_day` déclenché → un item
  `terms_violation_error_details` avec `metas.driver` / `metas.passenger` corrects et `metas.limit = 4`.
- Une règle sans metas (ex. `distance_too_short`) → item `{ label }` sans `metas`.
- Cumul de deux motifs → `terms_violation_labels` et `terms_violation_error_details` cohérents
  (même ensemble de labels, même ordre).
- Non-régression : `terms_violation_labels` conserve exactement la forme actuelle.

## Points à valider (revue)

1. Nom **`terms_violation_error_details`** + wrapper **`metas`** (alignement convention `anomaly_error_details`) —
   diffère du `terms_violation_details` / forme plate évoqués plus tôt.
2. `driver`/`passenger` = valeur brute de la règle (exclut le trajet courant), pas « total incluant ce trajet ».
