# Backfill historique complet du datalake — conçu pour tourner depuis un pod du cluster.
# Importé comme module `backfill` par le justfile racine => `just backfill <recette>`.

set dotenv-load
set shell := ["bash", "-uc"]

# DBT_TARGET_PATH / DBT_LOG_PATH : exportés par le justfile racine (redirection target/logs
# vers /tmp), hérités ici automatiquement en tant que module `backfill`.

# Profil mémoire borné repris par toutes les recettes : work_mem réduit + gather non
# parallèle bornent le pic mémoire (sinon work_mem × workers × threads => OOM, backend
# tué « SSL SYSCALL error: EOF »). use_remote_estimate + fetch_size sont posés côté
# SERVEUR (migration 0003), pas ici. synchronous_commit=off : sans risque, le backfill
# est rejouable (delete+insert idempotent par clé). Validé sur une année dense (2025).
PGOPTIONS_BOUNDED := "-c work_mem=96MB -c max_parallel_workers_per_gather=1 -c synchronous_commit=off"

default:
  @just --list backfill

# Orchestrateur bout-en-bout : migrations -> stats FDW -> geo -> trusted -> agrégé -> exposé.
# Prérequis : zone_raw déjà semée (`just pipeline-raw`). Idempotent et rejouable.
# ex : just backfill all   |   just backfill all 2019-01-01 2027-01-01
all start="2019-01-01" end="2027-01-01":
  #!/usr/bin/env bash
  set -euo pipefail
  just migrate
  just analyze-sources
  just backfill trusted-geo
  just backfill trusted {{start}} {{end}}
  just backfill aggregated {{start}} {{end}}
  just backfill exposed

# Couche geo/référence de trusted (perimeters, perimeters_agg, com_evolution) : tables non
# fenêtrées, bâties une fois. Prérequis de carpools et de toute la couche agrégée.
# Charge d'abord le seed aom_region (-> zone_trusted.aom_region), référence statique lue par
# la macro filtered_carpools (variantes AOM). Nécessaire car `dbt run` ne matérialise pas les
# seeds : sans ce pas, les ~100 modèles agrégés aom/aomreg échouent (relation inexistante).
trusted-geo *args:
  just dbt seed --select aom_region
  DBT_THREADS=1 just dbt run --select "tag:trusted,tag:geo" {{args}}

# Couche trusted incrémentale (5 modèles lus via FDW). MONO-THREAD volontaire : la
# concurrence sur le remote prod partagé dégrade chaque modèle (contention) et charge la
# prod. Chunk MENSUEL toujours (quelles que soient start/end) : borne le pic mémoire du pod
# (chunk annuel = OOM sur les années denses) et affine la granularité de reprise.
trusted start="2019-01-01" end="2027-01-01" *args:
  #!/usr/bin/env bash
  set -euo pipefail
  export PGOPTIONS="{{PGOPTIONS_BOUNDED}}"
  export DBT_THREADS=1
  just backfill-batch "models/trusted --exclude tag:geo" month {{start}} {{end}} {{args}}

# Couche agrégée (~470 modèles) : lectures LOCALES (plus de FDW) => MULTI-THREAD sûr.
# Deux passes (filtered_carpools -> time_filter honore --vars start/end), bornes étendues
# à des périodes entières : une tranche plus courte que le grain remplacerait sa ligne par
# le seul morceau calculé (delete+insert par clé).
# - MENSUELLE (au 1er du mois) pour jour/mois ;
# - ANNUELLE (au 1er janvier) pour trimestre/semestre/année.
# Les passes sont indépendantes : la seconde tourne même si la première a des échecs.
aggregated start="2019-01-01" end="2027-01-01" *args:
  #!/usr/bin/env bash
  set -euo pipefail
  export PGOPTIONS="{{PGOPTIONS_BOUNDED}}"
  export DBT_THREADS=6
  last_day=$(date -d "{{end}} -1 day" +%F)
  month_start="$(date -d "{{start}}" +%Y-%m)-01"
  month_end=$(date -d "$(date -d "$last_day" +%Y-%m)-01 +1 month" +%F)
  year_start="$(date -d "{{start}}" +%Y)-01-01"
  year_end="$(( $(date -d "$last_day" +%Y) + 1 ))-01-01"
  rc=0
  just backfill-batch "models/aggregated --exclude tag:quarter tag:semester tag:year" \
    month "$month_start" "$month_end" {{args}} || rc=1
  just backfill-batch \
    "models/aggregated,tag:quarter models/aggregated,tag:semester models/aggregated,tag:year" \
    year "$year_start" "$year_end" {{args}} || rc=1
  exit $rc

# Couche exposée (~23 modèles, lectures locales) : bâtie en une passe (multi-thread).
exposed *args:
  #!/usr/bin/env bash
  set -euo pipefail
  export PGOPTIONS="{{PGOPTIONS_BOUNDED}}"
  export DBT_THREADS=6
  just dbt run --select "models/exposed" {{args}}
