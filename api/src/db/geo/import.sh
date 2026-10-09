#!/usr/bin/env bash
# Import annuel des millésimes geo (dump `just export-perimeters` du datalake) dans la base de l'API.
# Connexion : variables libpq PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE (jamais dans argv/ps), ou
# APP_POSTGRES_URL qui est éclatée dans ces variables. Étapes séparées pour valider à la main entre
# deux (voir RUNBOOK.md).
set -euo pipefail

# readlink -f : dans l'image, le script est appelé via un lien symbolique dans le PATH.
here="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
dump_re='^perimeters_[0-9]{4}(-[0-9]{4})*\.[0-9]{8}T[0-9]{6}Z\.pgdump$'

usage() {
  cat >&2 <<USAGE
usage: import.sh <commande> [args]
  verify <source> [sha256]   télécharge et vérifie le dump, sans toucher la base
  stage <source> [sha256]    verify + restaure le dump dans geo_export (ne touche pas geo)
  check                      garde-fou + rapport sur geo_export, sans rien modifier
  apply                      remplace geo.perimeters et rafraîchit geo.com_evolution (une transaction)
  cleanup                    supprime le schéma geo_export
  run <source> [sha256]      stage + check + apply + cleanup

<source> : nom du dump (perimeters_2025-2026.20261001T080536Z.pgdump, lu sur
\$GEO_IMPORT_S3_BASE_URL/geo/), chemin d'un fichier local (./…) ou URL https.
Sans sha256, il est lu dans <source>.sha256 (sortie de \`just export-perimeters\`).
USAGE
  exit 2
}
[[ "${1:-}" =~ ^(verify|stage|check|apply|cleanup|run)$ ]] || usage

die() { echo "$*" >&2; exit 1; }

connect() {
  urldecode() { local s="${1//+/ }"; printf '%b' "${s//%/\\x}"; }
  if [[ -n "${APP_POSTGRES_URL:-}" ]]; then
    [[ "$APP_POSTGRES_URL" =~ ^postgres(ql)?://([^:@/]*)(:([^@/]*))?@([^:/?]+)(:([0-9]+))?/([^?]+)(\?(.*))?$ ]] \
      || die "APP_POSTGRES_URL illisible (attendu postgres://user:mdp@hôte:port/base)"
    export PGUSER="$(urldecode "${BASH_REMATCH[2]}")" PGPASSWORD="$(urldecode "${BASH_REMATCH[4]}")" \
      PGHOST="${BASH_REMATCH[5]}" PGPORT="${BASH_REMATCH[7]:-5432}" PGDATABASE="${BASH_REMATCH[8]}"
    [[ "${BASH_REMATCH[10]}" =~ sslmode=([a-z-]+) ]] && export PGSSLMODE="${BASH_REMATCH[1]}"
    unset APP_POSTGRES_URL
  fi
  [[ -n "${PGHOST:-}" && -n "${PGDATABASE:-}" ]] || die "connexion requise : PGHOST et PGDATABASE, ou APP_POSTGRES_URL"
}

sql() { psql -v ON_ERROR_STOP=1 "$@"; }

# Signé (SigV4) si la clé S3 est présente et l'URL sous GEO_IMPORT_S3_BASE_URL, anonyme sinon.
# Les identifiants passent par stdin (curl -K -) : printf est un builtin, aucun processus ne les
# porte en argument.
download() {
  local url="$1" dest="$2" base="${GEO_IMPORT_S3_BASE_URL:-}"
  echo "téléchargement de ${url%%\?*}" >&2
  if [[ -n "${AWS_ACCESS_KEY_ID:-}" && -n "${AWS_SECRET_ACCESS_KEY:-}" && -n "$base" && "$url" == "${base%/}/"* ]]; then
    local id="${AWS_ACCESS_KEY_ID//\\/\\\\}" key="${AWS_SECRET_ACCESS_KEY//\\/\\\\}"
    printf 'user = "%s:%s"\n' "${id//\"/\\\"}" "${key//\"/\\\"}" \
      | curl --proto '=https' --proto-redir '=https' -fsS --aws-sigv4 "aws:amz:fr-par:s3" -K - -o "$dest" "$url"
  else
    curl --proto '=https' --proto-redir '=https' -fsSL -o "$dest" "$url"
  fi
}

# verify <source> [sha256] : renseigne $dump (fichier vérifié) ; les téléchargements vont dans $work.
verify() {
  local source="${1:-}" sha="${2:-}" url="" expected origin
  [[ -n "$source" && ( -z "$sha" || "$sha" =~ ^[0-9a-f]{64}$ ) ]] || usage
  work="$(mktemp -d)"
  trap 'rm -rf "$work"' EXIT

  # Un nom nu désigne toujours S3 ; un fichier local se donne par un chemin (./, tmp/geo/…).
  if [[ "$source" == https://* ]]; then
    url="$source"
  elif [[ "$source" =~ $dump_re ]]; then
    [[ -n "${GEO_IMPORT_S3_BASE_URL:-}" ]] || die "GEO_IMPORT_S3_BASE_URL requis pour lire $source sur S3"
    url="${GEO_IMPORT_S3_BASE_URL%/}/geo/$source"
  elif [[ "$source" == */* && -f "$source" ]]; then
    dump="$source"
  else
    die "source inconnue : $source (ni nom de dump, ni fichier, ni URL https)"
  fi

  if [[ -n "$sha" ]]; then
    expected="$sha" origin="argument"
  else
    local sumfile="$work/dump.sha256"
    if [[ -n "$url" ]]; then
      download "${url%%\?*}.sha256" "$sumfile" || die "sha256 introuvable : ${url%%\?*}.sha256"
      origin="${url%%\?*}.sha256"
    else
      [[ -f "$source.sha256" ]] || die "sha256 introuvable : $source.sha256"
      cp "$source.sha256" "$sumfile"
      origin="fichier $(basename "$source").sha256"
    fi
    read -r expected _ < "$sumfile" || true
    [[ "$expected" =~ ^[0-9a-f]{64}$ ]] || die "sha256 mal formé dans $origin"
  fi
  echo "sha256 attendu : $expected ($origin)"

  if [[ -n "$url" ]]; then
    dump="$work/dump.pgdump"
    download "$url" "$dump" || die "téléchargement impossible : ${url%%\?*}"
  fi
  # Comparaison à la main : le sha256sum de BusyBox (image alpine) n'a pas --check/--quiet.
  [[ "$(sha256sum "$dump" | cut -d' ' -f1)" == "$expected" ]] || die "sha256 inattendu pour ${source%%\?*}"
  echo "dump vérifié : ${source%%\?*}"
}

stage() {
  connect
  verify "$@"
  sql -q -c "DROP SCHEMA IF EXISTS geo_export CASCADE; CREATE SCHEMA geo_export"
  pg_restore --no-owner --no-acl --exit-on-error -n geo_export -d "$PGDATABASE" "$dump"
  echo "geo_export restauré depuis ${1%%\?*}"
}

check() { connect; sql -f "$here/check.sql" -f "$here/report.sql"; }
apply() { connect; sql --single-transaction -f "$here/check.sql" -f "$here/import.sql" && echo "geo.perimeters remplacé"; }
cleanup() { connect; sql -q -c "DROP SCHEMA geo_export CASCADE"; }

case "$1" in
  verify) shift; verify "$@" ;;
  stage) shift; stage "$@" ;;
  check) check ;;
  apply) apply ;;
  cleanup) cleanup ;;
  run) shift; stage "$@"; check; apply; cleanup ;;
esac
