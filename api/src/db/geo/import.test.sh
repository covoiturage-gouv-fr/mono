#!/usr/bin/env bash
# Tests de `import.sh verify` (résolution de la source et du sha256), sans base ni réseau.
# Lancé par la CI dans l'image geo-import (BusyBox) : bash import.test.sh
set -uo pipefail

script="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/import.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
fails=0

# expect <code attendu> <motif attendu dans la sortie> -- <args de import.sh>
expect() {
  local code="$1" pattern="$2" out rc
  shift 3
  out="$(cd "$tmp" && env -u PGHOST -u APP_POSTGRES_URL bash "$script" "$@" 2>&1)"
  rc=$?
  if [[ "$rc" -ne "$code" || "$out" != *"$pattern"* ]]; then
    echo "KO: import.sh $* -> code $rc (attendu $code), sortie :" >&2
    echo "$out" | sed 's/^/    /' >&2
    fails=$((fails + 1))
  else
    echo "ok: import.sh $*"
  fi
}

name="perimeters_2025-2026.20261001T080536Z.pgdump"
printf 'dump factice\n' > "$tmp/$name"
sha="$(sha256sum "$tmp/$name" | cut -d' ' -f1)"
other="0000000000000000000000000000000000000000000000000000000000000000"

# Fichier local : sha lu dans le .sha256 voisin, au format sha256sum.
printf '%s  %s\n' "$sha" "$name" > "$tmp/$name.sha256"
expect 0 "$sha" -- verify "./$name"
expect 0 "fichier $name.sha256" -- verify "./$name"

# Le sha passé en argument l'emporte sur le .sha256.
printf '%s  %s\n' "$other" "$name" > "$tmp/$name.sha256"
expect 0 "argument" -- verify "./$name" "$sha"
expect 1 "sha256 inattendu" -- verify "./$name"
expect 1 "sha256 inattendu" -- verify "./$name" "$other"

# .sha256 mal formé ou absent.
printf 'pas-un-sha  %s\n' "$name" > "$tmp/$name.sha256"
expect 1 "mal formé" -- verify "./$name"
rm "$tmp/$name.sha256"
expect 1 "introuvable" -- verify "./$name"
expect 2 "usage" -- verify "./$name" "abc"

# Nom nu : URL construite depuis GEO_IMPORT_S3_BASE_URL + geo/ (port fermé : le téléchargement échoue).
GEO_IMPORT_S3_BASE_URL="https://127.0.0.1:9" expect 1 "https://127.0.0.1:9/geo/$name.sha256" -- verify "$name"
GEO_IMPORT_S3_BASE_URL="" expect 1 "GEO_IMPORT_S3_BASE_URL" -- verify "$name"

# Ni fichier, ni URL https, ni nom de dump valide : refusé avant tout téléchargement.
GEO_IMPORT_S3_BASE_URL="https://127.0.0.1:9" expect 1 "source inconnue" -- verify "../$name"
GEO_IMPORT_S3_BASE_URL="https://127.0.0.1:9" expect 1 "source inconnue" -- verify "geo/$name"
GEO_IMPORT_S3_BASE_URL="https://127.0.0.1:9" expect 1 "source inconnue" -- verify "http://example.org/$name"

# Les commandes qui touchent la base exigent toujours une connexion.
expect 1 "PGHOST" -- check

[[ "$fails" -eq 0 ]] && echo "tous les tests passent" || { echo "$fails échec(s)" >&2; exit 1; }
