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
trusted-geo *args:
  DBT_THREADS=1 just dbt run --select "tag:trusted,tag:geo" {{args}}

# Couche trusted incrémentale (5 modèles lus via FDW). MONO-THREAD volontaire : la
# concurrence sur le remote prod partagé dégrade chaque modèle (contention) et charge la
# prod. Chunk ANNUEL : la mémoire étant bornée, la taille de fenêtre ne joue plus que sur
# la durée par requête et la granularité de reprise.
trusted start="2019-01-01" end="2027-01-01" *args:
  #!/usr/bin/env bash
  set -euo pipefail
  export PGOPTIONS="{{PGOPTIONS_BOUNDED}}"
  export DBT_THREADS=1
  just backfill-batch "models/trusted --exclude tag:geo" year {{start}} {{end}} {{args}}

# Couche agrégée (~470 modèles) : lectures LOCALES (plus de FDW) => MULTI-THREAD sûr.
# Même fenêtrage annuel (filtered_carpools -> time_filter honore --vars start/end).
aggregated start="2019-01-01" end="2027-01-01" *args:
  #!/usr/bin/env bash
  set -euo pipefail
  export PGOPTIONS="{{PGOPTIONS_BOUNDED}}"
  export DBT_THREADS=6
  just backfill-batch "models/aggregated" year {{start}} {{end}} {{args}}

# Couche exposée (~23 modèles, lectures locales) : bâtie en une passe (multi-thread).
exposed *args:
  #!/usr/bin/env bash
  set -euo pipefail
  export PGOPTIONS="{{PGOPTIONS_BOUNDED}}"
  export DBT_THREADS=6
  just dbt run --select "models/exposed" {{args}}
