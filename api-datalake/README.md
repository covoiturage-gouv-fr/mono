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
      │  GET /observatory/*
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

## Modules

| Module | Rôle |
| --- | --- |
| `config.py` | Variables d'environnement (base `DBT_*`, Redis, cutoff, CORS) |
| `db.py` | Pool psycopg async (lecture seule) |
| `cache.py` | Clé de cache versionnée + gzip du payload |
| `period.py` | Fenêtre de publication + cutoff du dernier enregistrement |
| `helpers.py` | Allowlist des types de territoire |
| `repositories/observatory.py` | Requêtes SQL sur `zone_exposed` |
| `routers/observatory.py` | Endpoints publics |

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
- [x] Modèles dbt `zone_exposed.{location, observatory_perimeters, campaigns}` — validés sur prod
- [x] `GET /observatory/location` — heatmap H3, cache Redis gzip (SQL validé sur prod)
- [x] `GET /observatory/campaigns` — campagnes d'incitation + géométrie (SQL validé sur prod)
- [~] `GET /observatory/last-record` (canari) → `zone_exposed.od_month`
      (**bloqué** : `zone_aggregated`/`zone_exposed.od_*` pas encore matérialisés en prod)
- [ ] Endpoints `flux`, `incentive`, `distribution`, `occupation`, `keyfigures`, `infra`
      (bloqués sur la matérialisation de l'agrégé/exposé)
- [ ] Repointage `app-observatory` puis décommission du service Deno

> **Note prod** : au moment du dev, seuls `dlk_import`/`zone_raw`/`zone_trusted` sont
> construits en base. `location`, `perimeters` et `campaigns` dérivent de `zone_trusted`/
> `zone_raw` (dispos), d'où leur validation immédiate ; les endpoints agrégés attendent
> `dbt run --select aggregated exposed`.
