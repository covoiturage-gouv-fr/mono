# api-datalake

API de lecture de l'**observatoire public** du RPC, servie directement depuis le
datalake. Remplace le service `observatory` de l'API Deno, qui lisait les modèles
analytiques (`observatoire_stats.*`, `trusted_zone.*`, `raw_zone.*`) dans la base de
l'API — modèles désormais déménagés dans la base `datalake_production`.

**Stack :** Python 3.13 · uv · FastAPI · psycopg 3 (pool) · Redis (cache) · h3

## Pourquoi une API séparée

Le FDW datalake est **unidirectionnel** (le datalake lit l'API, jamais l'inverse).
L'observatoire est une API de lecture **synchrone** : impossible de l'inverser en
worker. On la déplace donc là où vivent les données — le datalake — pour lire
`zone_exposed.*` en local, sans requête cross-base.

## Architecture

```
app-observatory (Next.js)
      │  GET /v3/observatory/*
      ▼
api-datalake (FastAPI)  ──►  Redis (cache gzip, clé versionnée sur la publication)
      │  lecture locale
      ▼
datalake_production  ·  zone_exposed.{od,incentive,distribution,occupation,location}_*
```

- **Cache** : payload **gzippé** stocké tel quel dans Redis et servi en
  `Content-Encoding: gzip`. Clé = route + params + version de publication
  (cutoff `APP_OBSERVATORY_PUBLISHED_UNTIL` + compteur `obs:cache:version` que le
  pipeline dbt incrémente à chaque publication).
- **Fenêtre de publication** : `APP_OBSERVATORY_PUBLISHED_UNTIL` (borne supérieure
  exclusive, `YYYY-MM-DD`), portée depuis `helpers/publishedDate.ts`.

## Versioning

- **Contrat public = le préfixe d'URL `/v3`** (`API_VERSION` dans `main.py`). Toutes les
  routes observatoire sont servies sous `/v3/observatory/...`. Les changements **additifs**
  restent en `/v3` ; un changement **cassant** se fait en ajoutant `/v4` — on ne casse
  jamais `/v3` en place. Les sondes `/health` et `/health/ready` sont **hors contrat** (ops),
  non versionnées.
- **Artefact = tag d'image Docker** `datalake-api:<horodatage>-<sha court>` (build → commit).
  Pas de semver / release applicative (data-only, hors du gate semantic-release).

## Modules

| Module | Rôle |
| --- | --- |
| `config.py` | Variables d'environnement (base `DBT_*`, Redis, cutoff, CORS) |
| `access_log.py` | Journal d'accès JSON (une ligne par requête) |
| `db.py` | Pool psycopg async (lecture seule) |
| `cache.py` | Clé de cache versionnée + gzip du payload |
| `period.py` | Fenêtre de publication + cutoff du dernier enregistrement |
| `helpers.py` | Allowlist des types de territoire |
| `repositories/observatory.py` | Requêtes SQL sur `zone_exposed` |
| `routers/observatory.py` | Endpoints publics |

## Journal d'accès

Une ligne JSON par requête sur stdout, dans tous les environnements (sondes `/health` et
`/health/ready` exclues tant qu'elles réussissent). Il remplace le journal d'accès d'uvicorn (`--no-access-log`).

```json
{"ts": "2026-11-10T10:37:14.509+00:00", "level": "info", "ip": "203.0.113.7", "request_id": "0123abcd", "method": "GET", "route": "/observatory/location", "status": 200, "duration_ms": 65}
```

- `route` : gabarit de la route, absent sans correspondance (404, 503 de maintenance). Jamais
  de chemin brut, de query string, de corps, de cookie ni de jeton.
- `request_id` (`X-Request-Id`) et `pentest_id` (`X-Pentest-Id`) : absents si l'en-tête
  l'est ; valeurs réduites à `[A-Za-z0-9._-]`, 64 caractères au plus.
- `sub`, `role` : présents si l'authentification les a posés sur `request.state`.
- `ip` : uvicorn ne croit `X-Forwarded-For` que si le pair figure dans
  **`FORWARDED_ALLOW_IPS`** (IP ou CIDR, séparés par des virgules ; à renseigner avec le CIDR
  des pods nginx ingress). Défaut `127.0.0.1` : en-tête ignoré, `ip` = adresse du pair.

## Développement

```bash
just install          # uv sync
cp .env.example .env   # renseigner DBT_* / REDIS_URL
just dev               # http://localhost:8081
just test              # pytest
```

## État de la migration

> **Règle d'architecture** : l'API ne lit **que la zone `zone_exposed`**. Les données de
> référence dont elle a besoin (périmètres, campagnes) sont projetées dans l'exposé par des
> modèles dbt dédiés (`zone_exposed.observatory_perimeters`, `zone_exposed.campaigns`) — ce sont ces
> modèles qui lisent `zone_trusted`/`zone_raw`, jamais l'API.

- [x] Squelette FastAPI + cache + fenêtre de publication
- [x] Modèles dbt `zone_exposed.{location, observatory_perimeters, campaigns, aires_covoiturage}` — validés sur prod
- [x] `GET /v3/observatory/location`, `campaigns`, `last-record`
- [x] `GET /v3/observatory/territories/search` — autocomplete de territoires
      (modèle `zone_exposed.observatory_search_territories` + index GIN trigram,
      insensible aux accents ; remplace l'index Meilisearch `geo`)
- [x] **Endpoints agrégés** : `flux`, `best-flux`, `evol-flux`, `incentive`,
      `occupation`, `best-territories`, `evol-occupation`, `journeys-by-hours`,
      `journeys-by-distances`, `keyfigures`, `aires-covoiturage`
      (code + tests ; validation données au fil de la matérialisation de l'exposé)
- [ ] Repointage `app-observatory` puis décommission du service Deno
      (sélection de territoire repointée sur `/observatory/territories/search` ;
      la recherche de contenu du `SearchBar` reste sur Meilisearch, cf. #3358)

> **Note prod** : les endpoints agrégés lisent `zone_exposed.{od,occupation,distribution,
> incentive,users}_*`, matérialisés par le pipeline dbt (`dbt run --select aggregated exposed`).
> La parité des flux a été validée à ~91 % (région/dép) et 100 % (commune) contre l'API en
> ligne au grain agrégé ; les endpoints reproduisent cette donnée.

### Limites connues (endpoints agrégés)

- **`direction`** : les modèles exposés `occupation`/`incentive`/`users` viennent de
  `territory_month_*_both` → **direction `both` uniquement**. Le filtre `direction=from|to`
  du legacy est ignoré (émis en dur `both`). `distribution` a bien la dimension direction.
- **`keyfigures`** : recomposition (od + occupation + users), direction `both` — pas de
  modèle exposé dédié.
- **`evol-flux` indic `has_incentive`** : absent de `od_*` → retombe sur `journeys`.
- **Libellés EPCI/AOM** : forme IGN 2025 abrégée, différente du legacy (décision équipe en
  cours) — voir la tâche Notion dédiée.
