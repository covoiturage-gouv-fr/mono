"""Résolution des territoires custom (composites) — script one-shot, hors `dbt build`.

Lit les déclarations YAML de `pipelines/config/custom_territories/` (1 fichier = 1
composite), interroge `perimeters` au dernier millésime disponible pour résoudre chaque
membre (AOM / EPCI / dép / rég / commune) en liste de communes (`arr`), dédoublonne, et
écrit deux seeds figés et versionnés :

- `seeds/trusted/custom_territories.csv`      : (id, arr) — composition résolue.
- `seeds/trusted/custom_territories_meta.csv` : (id, libelle, active, earliest_safe_start).

Pourquoi one-shot et pas une résolution vivante dans un modèle dbt : le millésime est
figé au moment de la compilation, chaque évolution de composition est un diff Git
traçable, et `dbt build` n'a aucune dépendance de lecture sur `perimeters` pour ces seeds.

Usage :
    just custom-territories-compile          # (ré)écrit les seeds
    just custom-territories-compile check    # échoue si les seeds commités divergent
"""

import os
import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import psycopg
import typer
import yaml
from dotenv import load_dotenv

from pipelines.helpers.pg import pg_conninfo

load_dotenv()
app = typer.Typer()

_ROOT = Path(__file__).resolve().parents[2]
DECL_DIR = _ROOT / "pipelines" / "config" / "custom_territories"
SEED_DIR = _ROOT / "seeds" / "trusted"
MEMBERS_CSV = SEED_DIR / "custom_territories.csv"
META_CSV = SEED_DIR / "custom_territories_meta.csv"

# Types de membres acceptés dans un YAML. `arr` = identité (la valeur donnée EST déjà
# un code commune/arrondissement) ;
# les autres = colonne de `perimeters` filtrée, résolue vers ses communes (`arr`).
# `com` passe par la colonne car une commune peut porter plusieurs arrondissements
# (Paris/Lyon/Marseille). PERIMETER_COLUMNS sert d'allowlist : aucune valeur hors de là
# n'est jamais interpolée comme nom de colonne (garde-fou injection).
IDENTITY_TYPES = ("arr",)
PERIMETER_COLUMNS = ("com", "epci", "aom", "dep", "reg")
MEMBER_TYPES = IDENTITY_TYPES + PERIMETER_COLUMNS

SLUG_RE = re.compile(r"^[a-z0-9-]{3,32}$")
_SCHEMA_RE = re.compile(r"^[a-z_][a-z0-9_]*$")

# Plancher de backfill quand aucune fusion connue ne concerne les membres (= default_start
# du projet, cf. macros/filters/time_filter.sql).
DEFAULT_START = date(2019, 1, 1)
# mod INSEE : 31 fusion simple, 32 création de commune nouvelle, 33 fusion association.
FUSION_MODS = (31, 32, 33)

MEMBERS_HEADER = "id,arr"
META_HEADER = "id,libelle,active,earliest_safe_start"


@dataclass
class Declaration:
    id: str
    libelle: str
    active: bool
    members: dict[str, list[str]] = field(default_factory=dict)
    source: str = ""


# --------------------------------------------------------------------------- #
# Fonctions pures (testables sans DB)
# --------------------------------------------------------------------------- #
def validate_slug(value: str) -> None:
    if not SLUG_RE.match(value):
        raise ValueError(f"id invalide {value!r} : attendu ^[a-z0-9-]{{3,32}}$")
    if value.isdigit():
        raise ValueError(f"id invalide {value!r} : ne doit jamais être purement numérique")


def parse_declaration(raw: dict, source: str) -> Declaration:
    if not isinstance(raw, dict):
        raise ValueError(f"{source} : contenu YAML attendu = objet")
    territory_id = str(raw.get("id", "")).strip()
    validate_slug(territory_id)

    libelle = str(raw.get("libelle", "")).strip()
    if not libelle:
        raise ValueError(f"{source} : `libelle` manquant")

    active = raw.get("active", True)
    if not isinstance(active, bool):
        raise ValueError(f"{source} : `active` doit être un booléen")

    raw_members = raw.get("members") or {}
    if not isinstance(raw_members, dict):
        raise ValueError(f"{source} : `members` doit être un objet {{type: [codes]}}")

    unknown = set(raw_members) - set(MEMBER_TYPES)
    if unknown:
        raise ValueError(f"{source} : types de membres inconnus {sorted(unknown)} (attendus : {list(MEMBER_TYPES)})")

    members: dict[str, list[str]] = {}
    for mtype in MEMBER_TYPES:
        values = raw_members.get(mtype) or []
        if not isinstance(values, list):
            raise ValueError(f"{source} : members.{mtype} doit être une liste")
        cleaned = [str(v).strip() for v in values if str(v).strip()]
        if cleaned:
            members[mtype] = cleaned

    if not members:
        raise ValueError(f"{source} : au moins un membre est requis")

    return Declaration(id=territory_id, libelle=libelle, active=active, members=members, source=source)


def load_declarations(decl_dir: Path) -> list[Declaration]:
    decls: list[Declaration] = []
    for path in sorted(decl_dir.glob("*.yml")):
        raw = yaml.safe_load(path.read_text()) or {}
        decls.append(parse_declaration(raw, path.name))

    seen: dict[str, str] = {}
    for d in decls:
        if d.id in seen:
            raise ValueError(f"id dupliqué {d.id!r} : {seen[d.id]} et {d.source}")
        seen[d.id] = d.source
    return decls


def serialize_members(rows: list[tuple[str, str]]) -> str:
    ordered = sorted(set(rows))
    return "\n".join([MEMBERS_HEADER, *[f"{i},{a}" for i, a in ordered]]) + "\n"


def serialize_meta(rows: list[tuple[str, str, bool, date]]) -> str:
    lines = [META_HEADER]
    for territory_id, libelle, active, start in sorted(rows, key=lambda r: r[0]):
        lines.append(f"{territory_id},{_csv_field(libelle)},{str(active).lower()},{start.isoformat()}")
    return "\n".join(lines) + "\n"


def _csv_field(value: str) -> str:
    if any(ch in value for ch in (",", '"', "\n")):
        return '"' + value.replace('"', '""') + '"'
    return value


# --------------------------------------------------------------------------- #
# Accès DB
# --------------------------------------------------------------------------- #
def _schema() -> str:
    schema = os.getenv("CUSTOM_TERRITORIES_SCHEMA", "zone_trusted")
    if not _SCHEMA_RE.match(schema):
        raise ValueError(f"schéma invalide : {schema!r}")
    return schema


def latest_year(conn, schema: str) -> int:
    row = conn.execute(f"SELECT MAX(year) FROM {schema}.perimeters").fetchone()
    if not row or row[0] is None:
        raise RuntimeError(f"{schema}.perimeters vide — lancer d'abord `just pipeline-trusted-geo`")
    return int(row[0])


def resolve_members(conn, schema: str, year: int, decl: Declaration) -> set[str]:
    """Résout les membres d'un composite en un ensemble de communes (`arr`), dédoublonné."""
    arrs: set[str] = set()
    for mtype, codes in decl.members.items():
        if mtype in IDENTITY_TYPES:
            found = {
                r[0]
                for r in conn.execute(
                    f"SELECT DISTINCT arr FROM {schema}.perimeters WHERE year = %(y)s AND arr = ANY(%(codes)s)",
                    {"y": year, "codes": codes},
                ).fetchall()
            }
            missing = set(codes) - found
            if missing:
                raise ValueError(f"{decl.id} : commune(s) {mtype} introuvable(s) au millésime {year} : {sorted(missing)}")
            arrs |= found
        else:
            # mtype vient de PERIMETER_COLUMNS (allowlist) — jamais de la saisie libre.
            rows = conn.execute(
                f"SELECT {mtype} AS member, arr FROM {schema}.perimeters "
                f"WHERE year = %(y)s AND {mtype} = ANY(%(codes)s)",
                {"y": year, "codes": codes},
            ).fetchall()
            by_member: dict[str, set[str]] = {}
            for member, arr in rows:
                by_member.setdefault(str(member), set()).add(arr)
            unresolved = set(codes) - set(by_member)
            if unresolved:
                raise ValueError(f"{decl.id} : {mtype} sans commune rattachée au millésime {year} : {sorted(unresolved)}")
            for found in by_member.values():
                arrs |= found
    return arrs


def earliest_safe_start(conn, schema: str, arrs: set[str]) -> date:
    if not arrs:
        return DEFAULT_START
    row = conn.execute(
        f"SELECT MAX(date_eff) FROM {schema}.com_evolution "
        f"WHERE new_com = ANY(%(arrs)s) AND mod = ANY(%(mods)s)",
        {"arrs": sorted(arrs), "mods": list(FUSION_MODS)},
    ).fetchone()
    return row[0] if row and row[0] else DEFAULT_START


def compile_seeds(conn) -> tuple[str, str]:
    schema = _schema()
    year = latest_year(conn, schema)
    decls = load_declarations(DECL_DIR)

    member_rows: list[tuple[str, str]] = []
    meta_rows: list[tuple[str, str, bool, date]] = []
    for decl in decls:
        arrs = resolve_members(conn, schema, year, decl)
        if not arrs:
            raise ValueError(f"{decl.id} : aucune commune résolue")
        member_rows.extend((decl.id, arr) for arr in arrs)
        meta_rows.append((decl.id, decl.libelle, decl.active, earliest_safe_start(conn, schema, arrs)))

    return serialize_members(member_rows), serialize_meta(meta_rows)


# --------------------------------------------------------------------------- #
# Commandes
# --------------------------------------------------------------------------- #
@app.command()
def compile() -> None:  # noqa: A001 - nom de sous-commande CLI
    """Résout les déclarations YAML et (ré)écrit les seeds custom_territories*."""
    with psycopg.connect(pg_conninfo(), autocommit=True) as conn:
        members_csv, meta_csv = compile_seeds(conn)
    MEMBERS_CSV.write_text(members_csv)
    META_CSV.write_text(meta_csv)
    n_composites = meta_csv.count("\n") - 1
    n_members = members_csv.count("\n") - 1
    print(f"✅ {n_composites} composite(s), {n_members} commune(s) → {MEMBERS_CSV.name} / {META_CSV.name}")


@app.command()
def check() -> None:
    """Recompile en mémoire et échoue si les seeds commités divergent (garde-fou)."""
    with psycopg.connect(pg_conninfo(), autocommit=True) as conn:
        members_csv, meta_csv = compile_seeds(conn)
    drift = []
    for path, fresh in ((MEMBERS_CSV, members_csv), (META_CSV, meta_csv)):
        current = path.read_text() if path.exists() else ""
        if current != fresh:
            drift.append(path.name)
    if drift:
        print(f"❌ seed(s) divergent(s) : {', '.join(drift)} — lancer `just custom-territories-compile` et commiter le diff")
        raise typer.Exit(code=1)
    print("✅ seeds custom_territories à jour")


if __name__ == "__main__":
    app()
