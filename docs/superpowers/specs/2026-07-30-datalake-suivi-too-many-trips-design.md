# Design — Suivi datalake + Metabase des refus `too_many_trips_by_day` (GEN-647)

**Date** : 2026-07-30
**Ticket** : GEN-647 (suite)
**Objectif** : suivre l'évolution des trajets refusés `too_many_trips_by_day` et le rapport à leurs statuts, pour valider que le fix (comptage par usager) fonctionne.

## Décisions

| Point | Choix |
|---|---|
| Grain | Détail : une ligne par trajet refusé |
| Axe temps | `trip_day` (jour de départ) ; `refused_at` conservé en colonne |
| Périmètre | Tous opérateurs, opérateur en dimension |
| Lecture | Uniquement `trusted` (`ref('carpools')`), jamais `dlk_import`/`raw` |
| Persistance compteurs | Recalculés dans le modèle (pas de stockage côté OLTP) |

## Modèles

**`aggregated/terms_violation/too_many_trips_flagged`** (table incrémentale, `delete+insert` sur `carpool_id`, lookback 7 j, tag `daily`) — sur `ref('carpools')` :
- `carpool_id`, `operator_id`, `operator_name`, `trip_day`, `refused_at`, `acquisition_status`, `sole_reason`
- `driver_trips`, `passenger_trips` = `count(distinct operator_trip_id)` par usager / opérateur / jour, rôle conducteur ∪ passager, hors `canceled`
- `max_trips`, `wrongly_flagged` = `max_trips <= 4`

Clé usager = `md5(identity_key)` (colonnes `driver_key`/`passenger_key` de `carpools`), indépendante du rôle → comptage cross-rôle correct.

Incrémental 7 j : les refus récents restent à jour (les refus arrivent < 24 h après le trajet). Les statuts historiques ne se rafraîchissent qu'au `dbt run --full-refresh` — à lancer après la rétro-action.

**`app_metabase/mb_too_many_trips_flagged`** (vue, owner-rights) — `SELECT * FROM ref('too_many_trips_flagged')`, exposée à `datalake_mb_ro`.

## Validation

Logique validée sur le datalake (op 9 = BlaBlaCar Daily) : total ≈ 19 179 / à tort ≈ 17 670, cohérent avec le chiffrage prod_ro (19 151 / 17 653). Écart marginal = fuseau (geo-code local vs Europe/Paris).

## Dashboard Metabase (source Datalake, `app_metabase.mb_too_many_trips_flagged`)

1. **Validation du fix** — refus par semaine (`trip_day`), série `wrongly_flagged` vs légitime. La série à tort doit tomber à ~0 après déploiement du fix.
2. **Suivi des statuts** — trajets `wrongly_flagged` par `acquisition_status` (suit la rétro-action).
3. **KPI** — refus `wrongly_flagged` sur les 30 derniers jours (→ 0 post-fix).

Filtre opérateur transverse. Cartes fonctionnelles une fois les modèles déployés (dbt run en prod).

## Hors-scope

Rétro-action des trajets déjà refusés (script séparé, sous-page Notion) ; la mise à jour de leurs statuts dans ce modèle se fait via `--full-refresh`.
