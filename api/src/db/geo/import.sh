#!/usr/bin/env bash
# Import annuel des millésimes geo (dump `just export-perimeters` du datalake) dans la base de l'API.
# Connexion : APP_POSTGRES_URL. Étapes séparées pour valider à la main entre deux (voir RUNBOOK.md).
set -euo pipefail

# readlink -f : dans l'image, le script est appelé via un lien symbolique dans le PATH.
here="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
sql() { psql "$APP_POSTGRES_URL" -v ON_ERROR_STOP=1 "$@"; }

usage() {
  cat >&2 <<USAGE
usage: import.sh <commande> [args]
  stage <fichier|url> <sha256>   télécharge, vérifie, restaure le dump dans geo_export (ne touche pas geo)
  check                          garde-fou + rapport sur geo_export, sans rien modifier
  apply                          remplace geo.perimeters et rafraîchit geo.com_evolution (une transaction)
  cleanup                        supprime le schéma geo_export
  run <fichier|url> <sha256>     stage + check + apply + cleanup
USAGE
  exit 2
}

stage() {
  local source="${1:-}" sha="${2:-}" file
  [[ -n "$source" && "$sha" =~ ^[0-9a-f]{64}$ ]] || usage
  file="$source"
  if [[ "$source" == https://* ]]; then
    file="$(mktemp)"
    trap 'rm -f "$file"' EXIT
    curl --proto '=https' --proto-redir '=https' -fsSL -o "$file" "$source"
  fi
  # Comparaison à la main : le sha256sum de BusyBox (image alpine) n'a pas --check/--quiet.
  [[ "$(sha256sum "$file" | cut -d' ' -f1)" == "$sha" ]] || { echo "sha256 inattendu pour ${source%%\?*}" >&2; exit 1; }
  sql -q -c "DROP SCHEMA IF EXISTS geo_export CASCADE; CREATE SCHEMA geo_export"
  pg_restore --no-owner --no-acl --exit-on-error -n geo_export -d "$APP_POSTGRES_URL" "$file"
  echo "geo_export restauré depuis ${source%%\?*}"
}

check() { sql -f "$here/check.sql" -f "$here/report.sql"; }
apply() { sql --single-transaction -f "$here/check.sql" -f "$here/import.sql" && echo "geo.perimeters remplacé"; }
cleanup() { sql -q -c "DROP SCHEMA geo_export CASCADE"; }

case "${1:-}" in
  stage) shift; stage "$@" ;;
  check) check ;;
  apply) apply ;;
  cleanup) cleanup ;;
  run) shift; stage "$@"; check; apply; cleanup ;;
  *) usage ;;
esac
