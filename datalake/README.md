# Datalake

Système de transformation et d'exposition des données de covoiturage du RPC.
Remplace les projet legacy `sqlmesh/` et `dbt/`. Ingère les données brutes de l'API (trajets, opérateurs, fraude, CEE), les enrichit géographiquement, les agrège à plusieurs grains temporels et géographiques, et les expose pour les dashboards publics et les exports open data.

**Stack :** dbt-core + dbt-postgres · Python 3.13 + uv · DuckDB (ETL) · PostgreSQL 17 · boto3 (S3) · Elementary (qualité données)

---

## Architecture — 4 zones en médaillon

```
┌─────────────────────────────────────────────────────────────────────────┐
│  SOURCES EXTERNES                                                       │
│  ┌────────────────┐  ┌──────────────────┐  ┌───────────────────────┐    │
│  │  API interne   │  │  IGN / CEREMA    │  │  data.gouv.fr / INSEE │    │
│  │  (carpool_v2,  │  │  (GeoPackage     │  │  (campagnes, aires,   │    │
│  │  cee, policy,  │  │  communes, EPCIs,│  │  mouvements communes) │    │
│  │  fraudcheck…)  │  │  depts, régions) │  │                       │    │
│  └───────┬────────┘  └───────┬──────────┘  └──────────┬────────────┘    │
└──────────┼───────────────────┼────────────────────────┼─────────────────┘
           │ postgres_fdw      │ seed_perimeters.json   │ seed_datagouv.json
           │ (dlk_import)      │ (S3 → DuckDB → PG)     │ (fetch live → PG)
           │                   └────────────┬───────────┘
           │                                ▼
           │             ┌────────────────────────────────────────────────┐
           │             │  ZONE RAW  (zone_raw)                          │
           │             │  Sources géo / open-data, telles quelles       │
           │             │  communes · EPCIs · depts · régions ·          │
           │             │  aires · campagnes · mouvements communes       │
           │             └──────────────────┬─────────────────────────────┘
           │  carpool_v1/v2 · cee ·         │ dbt run --select trusted.*
           │  policy · fraudcheck ·         │
           │  anomaly · operator ·          │
           │  company · territory           │
           ▼                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  ZONE TRUSTED  (zone_trusted)  — 8 modèles                              │
│  Nettoyage, enrichissement géographique, dédoublonnage                  │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │  carpools  (incrémental, lookback 3 jours)                      │    │
│  │  • Jointures: status + correction_geo + fraude + anomalie       │    │
│  │  • Enrichit : timezone, H3 z8/z9, hash driver/passager          │    │
│  │  • Incentive: incentives opérateur + campagnes                  │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│  perimeters · perimeters_agg · incentives · operator_incentives         │
│  cee · com_evolution · carpools_geo_correction                          │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │  dbt run --select aggregated.*
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  ZONE AGGREGATED  (zone_aggregated)  — 469 modèles générés par macros   │
│                                                                         │
│  Grains temporels : day · month · quarter · semester · year             │
│  Périmètres géo   : arr · com · plm · epci · aom · dep · reg · pays     │
│  Directions       : from (départ) · to (arrivée) · both (les deux)      │
│                                                                         │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │  OD (Origin-Destination) · 45 modèles                          │     │
│  │  Clés : start_code, end_code, grain temporel                   │     │
│  │  Métriques :                                                   │     │
│  │  • Volumes      carpools, trips, unique_drivers/passengers     │     │
│  │  • Nouveaux     carpools/count new_drivers, new_passengers     │     │
│  │  • Financier    driver_revenue, passenger_contribution         │     │
│  │  • Géo          distance, duration                             │     │
│  │  •Incitations:                                                 │     │
│  │    • Opérateurs   oi_collectivite/operator/other + amounts     │     │
│  │    • RPC          ci_collectivite (campagnes), amounts/results │     │
│  │  • Distrib.     hours_distribution[24h], oc_distribution[A/B/C]│     │
│  └────────────────────────────────────────────────────────────────┘     │
│                                                                         │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │  Fraud · 141 modèles  (fraud_<grain>_<perim>_<direction>)      │     │
│  │  Clés : operator_id, operator_name, code géo, grain temporel   │     │
│  │  Métriques :                                                   │     │
│  │  • carpools           total tous statuts                       │     │
│  │  • carpools_valid     acquisition OK                           │     │
│  │  • carpools_invalid   acquisition KO                           │     │
│  │  • carpools_fraud     label fraude = 'failed'                  │     │
│  │  • carpools_anomaly   label anomalie = 'failed'                │     │
│  │  • carpools_terms_violation  erreur CGU                        │     │
│  └────────────────────────────────────────────────────────────────┘     │
│                                                                         │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │  Territory · 141 modèles  (territory_<grain>_<perim>_<dir>)    │     │
│  │  Clés : code géo (start ou end selon direction), grain         │     │
│  │  Métriques : même base que OD, plus :                          │     │
│  │  • intra_carpools     trajets dont départ = arrivée            │     │
│  │  • passenger_over_18, passenger_seats                          │     │
│  │  • dist_distribution  répartition en 17 tranches (0-5 km … 80+)│     │
│  │  (sans ventilation start/end — vue d'un territoire donné)      │     │
│  └────────────────────────────────────────────────────────────────┘     │
│                                                                         │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │  Operators · 138 modèles  (operators_<grain>_<perim>_<dir>)    │     │
│  │  Clés : operator_id, operator_name, code géo, grain            │     │
│  │  Métriques : identiques à Territory (même macro)               │     │
│  │  → même colonnes mais ventilées par opérateur                  │     │
│  └────────────────────────────────────────────────────────────────┘     │
│                                                                         │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │  Users · 4 modèles                                             │     │
│  │  users            user_id · 1re/dernière date conducteur       │     │
│  │                   et passager · geo_code de 1re activité       │     │
│  │  user_od_day/month  activité journalière/mensuelle par rôle    │     │
│  │                   avec métriques OD (distance, revenus, CI…)   │     │
│  │  user_cee_new_drivers  conducteurs éligibles CEE (1er trajet)  │     │
│  └────────────────────────────────────────────────────────────────┘     │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │  dbt run --select exposed.*
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  ZONE EXPOSED  (zone_exposed)  — 22 modèles                             │
│                                                                         │
│  Observatory · 20 modèles  (4 grains : mois/trimestre/semestre/année)   │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │  od_*          flux OD entre territoires                       │     │
│  │                journeys, passengers, distance, duration        │     │
│  │                + libellés et coordonnées centroïdes (lng/lat)  │     │
│  └────────────────────────────────────────────────────────────────┘     │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │  occupation_*  taux d'occupation par territoire                │     │
│  │                journeys, trips, has_incentive, occupation_rate │     │
│  │                + géométrie GeoJSON du périmètre                │     │
│  └────────────────────────────────────────────────────────────────┘     │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │  incentive_*   répartition des incitations par territoire      │     │
│  │                collectivite, operateur, autres, no_incentive   │     │
│  └────────────────────────────────────────────────────────────────┘     │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │  distribution_*  distributions horaires et kilométriques       │     │
│  │                  hours[] JSONB · 24 créneaux par heure         │     │
│  │                  distances[] JSONB · 6 tranches (0-10 … >50km) │     │
│  │                  par direction (from / to / both)              │     │
│  └────────────────────────────────────────────────────────────────┘     │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │  users_*       conducteurs et passagers actifs par territoire  │     │
│  │                unique_drivers, new_drivers                     │     │
│  │                unique_passengers, new_passengers               │     │
│  └────────────────────────────────────────────────────────────────┘     │
│                                                                         │
│  Export · 2 modèles  (vues, filtré sur valid_acquisition_status)        │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │  export_opendata   trajet anonymisé pour data.gouv.fr          │     │
│  │                    coords arrondies (2-3 déc.), heure arrondie │     │
│  │                    à 10 min, INSEE start/end, distance km      │     │
│  └────────────────────────────────────────────────────────────────┘     │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │  export_partners   trajet enrichi pour les AOM partenaires     │     │
│  │                    codes insee/epci/aom/dep/reg start & end    │     │
│  │                    identités opérateur · OI×3 + CI×3 dépliés   │     │
│  │                    cee_application, driver_revenue, distance   │     │
│  └────────────────────────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Notion de direction : from / to / both

Les modèles `aggregated` et `exposed` sont déclinés selon **trois directions**, qui définissent le point de vue géographique sous lequel un trajet est comptabilisé :

| Direction | Signification                                                                                                                                            | Exemple                                                        |
| --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| `from`    | Le trajet est compté dans le territoire de **départ**                                                                                                    | un trajet Lyon → Paris est dans `dep_from` de Lyon             |
| `to`      | Le trajet est compté dans le territoire d'**arrivée**                                                                                                    | le même trajet est dans `dep_to` de Paris                      |
| `both`    | Le trajet est compté **deux fois** : dans le territoire de départ ET dans celui d'arrivée, sauf s'il est intra (départ = arrivée, compté une seule fois) | utilisé pour les statistiques de fréquentation d'un territoire |

**Conséquence :** `SUM(carpools) sur both` ≈ `SUM(from) + SUM(to)` pour les trajets non intra. Les modèles `both` sont donc la vue d'un territoire indépendamment du sens du trajet, utilisée dans la plupart des dashboards.

---

## Macros

Toute la génération de code des 469 modèles agrégés repose sur des macros Jinja organisées en trois familles.

### Macros de génération de modèles

Ces macros constituent le corps entier d'un fichier `.sql` — un appel suffit à générer un modèle complet avec sa configuration dbt, ses index et sa logique SQL.

| Macro             | Signature                   | Rôle                                                                                                                                                                      |
| ----------------- | --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `od_model`        | `(perim, grain)`            | Agrégation OD : paire `(territory_1, territory_2)` ordonnée par LEAST/GREATEST pour éviter les doublons directionnels. `com` = vue UNION ALL de `arr` + `plm`.            |
| `territory_model` | `(perim, grain, direction)` | Agrégation par territoire. `both` = UNION ALL start + end en excluant les intra sur le second membre. Mode strict : les codes hors-périmètre sont NULL (pas de fallback). |
| `fraud_model`     | `(perim, grain, direction)` | Identique à `territory_model` mais sans filtre `valid_acquisition_status` — comptabilise tous les statuts pour mesurer fraudes et anomalies.                              |
| `operators_model` | `(perim, grain, direction)` | Identique à `territory_model` avec `operator_id` et `operator_name` en clés supplémentaires.                                                                              |

Chaque macro embarque sa propre table de lookbacks :

| Grain      | Lookback                          |
| ---------- | --------------------------------- |
| `day`      | 3 jours                           |
| `month`    | 1 mois                            |
| `quarter`  | 1 trimestre                       |
| `semester` | 0 (full depuis début du semestre) |
| `year`     | 0 (full depuis début de l'année)  |

### Macros de colonnes agrégées

Injectées dans les SELECT via `{{ macro_name() }}`.

| Macro                     | Colonnes produites                                                                                                                                    |
| ------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `od_agg_columns()`        | carpools, trips, unique/new drivers & passengers, revenue, contribution, distance, duration, OI×3, CI, hours_distribution[24], oc_distribution[A/B/C] |
| `territory_agg_columns()` | idem OD + intra_carpools, passenger_over_18, passenger_seats, dist_distribution[17 tranches 0-5km…80+]                                                |
| `fraud_agg_columns()`     | carpools, carpools_valid, carpools_invalid, carpools_fraud, carpools_anomaly, carpools_terms_violation                                                |
| `user_agg_columns()`      | carpools, trips, distance, duration, revenue, contribution, OI×3, CI, hours_distribution[24], oc_distribution[A/B/C]                                  |

### Macros filtres et helpers

**`filtered_carpools(perim, ...)`** — CTE source de tous les modèles agrégés. Centralise la jointure temporelle sur `perimeters` (via `valid_from`/`valid_until`), le calcul du code géo selon le périmètre, le flag `is_intra`, le filtre `valid_acquisition_status`, et la jointure `users` pour `is_new_driver`/`is_new_passenger`.

**`time_filter(column, ...)`** — filtre temporel adaptatif : `MAX(incremental_date) - lookback` en mode incrémental, `default_start` en full-refresh. Surchargeable via `--vars 'start: 2024-01-01'` pour les backfills ciblés.

**`window_start(model_ref, column, ...)`** — calcule le début de fenêtre incrémentale : `MAX(incremental_date) - lookback_interval`, avec fallback sur `default_start` si la table est vide.

**`incremental_columns(date_col, grain)`** — génère les colonnes de date selon le grain : `incremental_date` seul pour `day`, plus `year`+`month`/`quarter`/`semester` pour les autres.

**`timezoned_ts(geo_code_col, datetime_col)`** — convertit UTC en heure locale selon le code géo. Gère les DOM-TOM : Guadeloupe/Martinique (971–972), Guyane (973), Réunion (974), Mayotte (976). Défaut : Europe/Paris.

**`delete_window(path_prefix, start, end)`** — supprime les données d'une fenêtre temporelle dans tous les modèles incrémentiaux d'un répertoire. Invoqué par `just dbt-recompute` avant de relancer les modèles.

**`carpools_perim_join()`** — fragment SQL de double LEFT JOIN sur `perimeters` (start et end) avec filtre `valid_from`/`valid_until`, réutilisé dans `filtered_carpools`.

**`group_by_grain(grain, start_index)`** — génère les positions de colonnes pour le `GROUP BY` selon le grain (le nombre de colonnes de date varie).

---

## Premier remplissage — initialisation complète

Séquence à exécuter **une seule fois** pour peupler le datalake from scratch.

### Étape 1 — Données géographiques de référence

```bash
just pipeline-raw
```

Charge via DuckDB les données IGN 2025 (GPKG), CEREMA AOMs et mouvements INSEE depuis `<bucket>/seeds/` vers `zone_raw`. Chunk size : 10 000 lignes. Inclut géométries complètes, simplifiées et centroïdes pour les communes, EPCIs, départements et régions. Charge aussi les campagnes et aires de covoiturage **récupérées en direct** depuis data.gouv.fr / transport.data.gouv.fr : `get_last_url` interroge l'API à chaque run et lit la dernière ressource publiée (pas de copie dans le bucket).

### Étape 2 — Zone trusted géographique

```bash
just pipeline-trusted-geo
```

Construit la hiérarchie géographique dans `zone_trusted` (`perimeters`, `perimeters_agg`, `com_evolution`). Base pour tous les JOINs géographiques des carpools.

### Étape 3 — Backfill des carpools

```bash
just backfill-batch trusted.carpools month 2020-01 2026-06
```

Rejoue mois par mois (delete + insert par fenêtre) pour éviter les "phantom rows" sur les grands volumes. Enrichit chaque fenêtre avec la correction géo, les labels fraude/anomalie, les incentives et les campagnes.

### Étape 4 — Agrégations historiques complètes

```bash
just backfill-batch aggregated.* month 2020-01 2026-06
```

Lance les 469 modèles agrégés sur toute la période. Les macros `od_model` / `fraud_model` / `territory_model` tournent en parallèle (`DBT_THREADS=6` par défaut).

### Étape 5 — Zone exposed

```bash
dbt run --select exposed.*
```

Matérialise les 22 tables observatory et export, disponibles pour `app-observatory` et les exports S3.

---

## Pipeline quotidien

```
just pipeline-daily
= dbt run --select tag:daily

  trusted.carpools          ← delete + insert sur D-3 → today
  trusted.incentives
  trusted.cee
        │
        ▼
  aggregated.*  grain=day   ← lookback 3 jours
  (+ refresh month          ← lookback 1 mois si 1er du mois)
        │
        ▼
  exposed.observatory.*     ← recalcul du mois courant
  exposed.export.*          ← exports S3
        │
        ▼
  dbt test (Elementary)     ← carpools_row_count, key_fields, missing_rows…
```

**Pourquoi le lookback 3 jours ?** Les données de l'API peuvent arriver avec du retard (corrections géo, labels fraude). Supprimer et réinsérer les 3 derniers jours à chaque run garantit la cohérence sans full-refresh.

---

## Commandes clés

| Action                               | Commande                                                     |
| ------------------------------------ | ------------------------------------------------------------ |
| Lister toutes les commandes          | `just`                                                       |
| Pipeline du jour                     | `just pipeline-daily`                                        |
| Backfill d'un modèle sur une période | `just backfill-batch trusted.carpools month 2024-01 2024-12` |
| Recompute d'une fenêtre précise      | `just dbt-recompute trusted.carpools 2024-06-01 2024-06-30`  |
| Docs dbt en local                    | `just docs-serve`                                            |
| Lint SQL                             | `just lint`                                                  |
| Générer les YAMLs d'un layer         | `just osmosis-refactor trusted`                              |

---

## Développement local

**Prérequis :** Python 3.13, uv, Docker, direnv

**Setup :**

1. `direnv allow` — active le virtualenv automatiquement au `cd`
2. `cp .env.example .env` et renseigner les variables (voir `.env.example`)
3. `just` pour lister les commandes disponibles

**Postgres local :**

```bash
docker compose up -d postgres
```

---

## CI/CD

### Validation PR (`.github/workflows/quality-datalake.yml`)

Déclenché sur chaque PR touchant `datalake/`. Quatre jobs parallèles :

- `uv lock` — vérifie que le lockfile est à jour
- Dockerfile build — smoke test de l'image de production
- `dbt parse` — valide la syntaxe et les références des modèles
- sqlfluff lint — contrôle le style SQL (non-bloquant)

### Déploiement (`.github/workflows/deploy-datalake.yml`)

Déclenché sur push vers `main` (production) ou `datalake` (staging). Construit et pousse une image Docker vers :

```text
ghcr.io/betagouv/preuve-covoiturage/datalake:YYYY-MM-DD.HHMM
```

Déploiement FluxCD sur Kubernetes géré dans le dépôt ops (séparé).

---

## Commandes dbt utiles

Générer un YAML sources depuis une table :

```bash
dbt run-operation generate_source --args '{"schema_name": "fraudcheck", "database_name": "local","generate_columns": true, "table_names":["labels"]}' --profiles-dir ./profiles/
```

Générer un YAML modèle depuis un modèle existant :

```bash
dbt run-operation generate_model_yaml --args '{"model_names": ["interoperators_labels_by_month"]}' --profiles-dir ./profiles/
```

---

## Glossaire

| Terme                             | Définition                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| --------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **OI**                            | _Operator Incentive_ — incitation versée par un opérateur ou une collectivité **en dehors** du RPC, déclarée par l'opérateur dans la déclaration de trajet. Subdivisée en `oi_collectivite` (versée par une AOM), `oi_operator` (versée par l'opérateur lui-même), `oi_other` (autre).                                                                                                                                                                                                                                                                                                                                                                   |
| **CI**                            | _Campaign Incentive_ — incitation calculée et versée **par le RPC** via une campagne incitative (`campaigns` JSONB). Comptée dans `ci_collectivite`, `ci_amount_total`, `ci_result_total`.                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| **Classe opérateur A/B/C**        | Niveau de certification de l'opérateur par le RPC. A = certification légère, B = intermédiaire, C = maximale (contrôle d'identité, preuve forte). Impacte l'accès aux incitations.                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| **acquisition_status**            | Statut de la déclaration du trajet : `ok` (accepté), `terms_violation_error` (CGU non respectées), autres codes d'erreur d'acquisition.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| **valid_acquisition_status**      | Booléen dérivé de `acquisition_status` : `true` si le trajet est valide pour les statistiques et exports. Filtre de base dans `export_opendata` et `export_partners`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| **fraud_status / anomaly_status** | Labels issus des moteurs de détection. `failed` = détection positive (fraude ou anomalie avérée).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| **is_intra**                      | `true` si le code géo de départ = code géo d'arrivée au niveau considéré. Ces trajets sont comptés une seule fois dans les agrégats `both`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| **H3 z8 / z9**                    | Indexation géospatiale hexagonale (bibliothèque H3 d'Uber). z9 ≈ maille de ~0,1 km², z8 ≈ 0,7 km². Utilisée pour les agrégats fraude et la détection de patterns géographiques.                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| **arr / com / plm**               | Trois vues du niveau communal. `arr` = maille la plus fine : arrondissement municipal pour Paris (75101…), Lyon (69381…) et Marseille (13201…), commune IGN pour tout le reste. `plm` = Paris, Lyon et Marseille vus comme une **commune entière** (75056, 69123, 13055), agrégation de leurs arrondissements. `com` = UNION de `arr` + `plm` : toutes les communes de France, avec les 3 villes PLM présentes **deux fois** (une fois découpées en arrondissements via `arr`, une fois en tant que ville entière via `plm`).                                                                                                                            |
| **epci / aom / aomreg**           | `epci` = Établissement Public de Coopération Intercommunale. `aom` = Autorité Organisatrice de la Mobilité (source CEREMA) — **exclut les 14 AOMs régionales** (filtrées via `aom_region`). `aomreg` = ces 14 AOMs régionales uniquement (régions qui exercent elles-mêmes la compétence mobilité : Bretagne, Occitanie, Normandie…), avec les **trajets intra-AOM exclus** : à l'échelle d'une région entière, comptabiliser les trajets dont le départ et l'arrivée sont dans la même AOM noierait les flux inter-territoires. Les deux niveaux sont complémentaires et couvrent ensemble toutes les AOMs. Défini dans `seeds/trusted/aom_region.csv`. |
| **CEE**                           | _Certificat d'Économie d'Énergie_ — dispositif réglementaire pour lequel certains trajets sont éligibles. `user_cee_new_drivers` liste les conducteurs effectuant leur premier trajet éligible CEE.                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| **Lookback**                      | Fenêtre temporelle sur laquelle un modèle incrémental supprime et reinsère les données à chaque run, pour absorber les corrections tardives de l'API.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |

---

## Contribuer — ajouter un modèle

### Nouveau périmètre géographique

1. Ajouter la couche source dans `pipelines/config/raw/seed_perimeters.json`
2. Déclarer la table dans `models/sources/raw.yml`
3. Ajouter le code dans `models/trusted/perimeters.sql` et `perimeters_agg.sql`
4. Créer les fichiers modèles agrégés en appelant les macros existantes :

```sql
-- models/aggregated/territory/day/territory_day_<perim>_from.sql
{{ territory_model('newperim', 'day', 'from') }}
```

1. Répéter pour les 5 grains × 3 directions = 15 fichiers (utiliser un script shell ou copier depuis un périmètre existant)
1. Ajouter le périmètre dans les `type_map` des modèles `exposed/observatory/`

### Nouveau grain temporel

1. Ajouter l'entrée dans le dict `lookbacks` de chaque macro (`od_model`, `fraud_model`, `territory_model`, `operators_model`)
2. Ajouter le grain dans la macro `incremental_columns` (`macros/helpers/incremental_columns.sql`) et `group_by_grain`
3. Créer les fichiers modèles (un par périmètre × direction) dans le sous-dossier correspondant

### Nommage des fichiers

```text
models/aggregated/<famille>/<grain>/<famille>_<grain>_<perim>_<direction>.sql
```

Exemple : `models/aggregated/territory/month/territory_month_epci_both.sql`

Le contenu est toujours une simple invocation de macro :

```sql
{{ territory_model('epci', 'month', 'both') }}
```
