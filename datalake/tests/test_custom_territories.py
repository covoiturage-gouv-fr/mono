from datetime import date

import pytest

from pipelines.cmd.custom_territories import (
    DEFAULT_START,
    Declaration,
    earliest_safe_start,
    load_declarations,
    parse_declaration,
    resolve_members,
    serialize_members,
    serialize_meta,
    validate_slug,
)


# --------------------------------------------------------------------------- #
# validate_slug
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("value", ["pole-metropolitain", "abc", "a1-b2-c3", "x" * 32])
def test_validate_slug_accepts(value):
    validate_slug(value)


@pytest.mark.parametrize(
    "value",
    ["ab", "x" * 33, "Pole", "pole_metro", "pôle", "12345", "with space", ""],
)
def test_validate_slug_rejects(value):
    with pytest.raises(ValueError):
        validate_slug(value)


# --------------------------------------------------------------------------- #
# parse_declaration
# --------------------------------------------------------------------------- #
def _raw(**over):
    base = {"id": "pole-x", "libelle": "Pôle X", "active": True, "members": {"epci": [123]}}
    base.update(over)
    return base


def test_parse_declaration_ok():
    d = parse_declaration(_raw(members={"epci": [123, " 456 "], "aom": [], "arr": ["2A004"]}), "pole-x.yml")
    assert d.id == "pole-x"
    assert d.members == {"epci": ["123", "456"], "arr": ["2A004"]}
    assert d.active is True


def test_parse_declaration_requires_libelle():
    with pytest.raises(ValueError, match="libelle"):
        parse_declaration(_raw(libelle="  "), "f.yml")


def test_parse_declaration_requires_a_member():
    with pytest.raises(ValueError, match="au moins un membre"):
        parse_declaration(_raw(members={"aom": [], "epci": []}), "f.yml")


def test_parse_declaration_rejects_unknown_member_type():
    with pytest.raises(ValueError, match="inconnus"):
        parse_declaration(_raw(members={"canton": [1]}), "f.yml")


def test_parse_declaration_rejects_non_bool_active():
    with pytest.raises(ValueError, match="active"):
        parse_declaration(_raw(active="yes"), "f.yml")


# --------------------------------------------------------------------------- #
# load_declarations
# --------------------------------------------------------------------------- #
def test_load_declarations_reads_only_yml_and_detects_dupes(tmp_path):
    (tmp_path / "a.yml").write_text("id: pole-a\nlibelle: A\nmembers:\n  aom: [1]\n")
    (tmp_path / "b.yml.example").write_text("id: pole-a\nlibelle: dupe ignoree\nmembers:\n  aom: [2]\n")
    decls = load_declarations(tmp_path)
    assert [d.id for d in decls] == ["pole-a"]


def test_load_declarations_raises_on_duplicate_id(tmp_path):
    (tmp_path / "a.yml").write_text("id: pole-a\nlibelle: A\nmembers:\n  aom: [1]\n")
    (tmp_path / "b.yml").write_text("id: pole-a\nlibelle: B\nmembers:\n  aom: [2]\n")
    with pytest.raises(ValueError, match="dupliqué"):
        load_declarations(tmp_path)


# --------------------------------------------------------------------------- #
# serialisation
# --------------------------------------------------------------------------- #
def test_serialize_members_sorts_and_dedupes():
    out = serialize_members([("pole-b", "75056"), ("pole-a", "38240"), ("pole-a", "38240")])
    assert out == "id,arr\npole-a,38240\npole-b,75056\n"


def test_serialize_members_header_only_when_empty():
    assert serialize_members([]) == "id,arr\n"


def test_serialize_meta_formats_bool_and_date_and_quotes():
    out = serialize_meta(
        [
            ("pole-b", "Sans virgule", True, date(2019, 1, 1)),
            ("pole-a", "Avec, virgule", False, date(2021, 3, 4)),
        ]
    )
    assert out == (
        "id,libelle,active,earliest_safe_start\n"
        'pole-a,"Avec, virgule",false,2021-03-04\n'
        "pole-b,Sans virgule,true,2019-01-01\n"
    )


# --------------------------------------------------------------------------- #
# resolve_members / earliest_safe_start (DB mockée)
# --------------------------------------------------------------------------- #
class FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class FakeConn:
    """Renvoie des lignes scriptées selon un fragment de requête."""

    def __init__(self, script):
        self.script = script
        self.seen = []

    def execute(self, sql, params=None):
        self.seen.append((sql, params))
        for fragment, rows in self.script.items():
            if fragment in sql:
                return FakeCursor(rows)
        return FakeCursor([])


def test_resolve_members_dedupes_overlap():
    decl = Declaration(id="pole-x", libelle="X", active=True, members={"epci": ["1"], "aom": ["9"]})
    conn = FakeConn(
        {
            "epci = ANY": [("1", "38001"), ("1", "38002")],
            "aom = ANY": [("9", "38002"), ("9", "38003")],
        }
    )
    assert resolve_members(conn, "zone_trusted", 2026, decl) == {"38001", "38002", "38003"}


def test_resolve_members_com_resolves_via_column():
    # une commune "chef-lieu" (ex. Marseille) porte plusieurs arrondissements
    decl = Declaration(id="pole-x", libelle="X", active=True, members={"com": ["13055"]})
    conn = FakeConn({"com = ANY": [("13055", "13201"), ("13055", "13202")]})
    assert resolve_members(conn, "zone_trusted", 2026, decl) == {"13201", "13202"}


def test_resolve_members_fails_on_unknown_commune():
    decl = Declaration(id="pole-x", libelle="X", active=True, members={"arr": ["99999"]})
    conn = FakeConn({"arr = ANY": []})
    with pytest.raises(ValueError, match="introuvable"):
        resolve_members(conn, "zone_trusted", 2026, decl)


def test_resolve_members_fails_on_epci_without_communes():
    decl = Declaration(id="pole-x", libelle="X", active=True, members={"epci": ["1", "2"]})
    conn = FakeConn({"epci = ANY": [("1", "38001")]})
    with pytest.raises(ValueError, match="sans commune"):
        resolve_members(conn, "zone_trusted", 2026, decl)


def test_earliest_safe_start_uses_fusion_date():
    conn = FakeConn({"com_evolution": [(date(2024, 1, 1),)]})
    assert earliest_safe_start(conn, "zone_trusted", {"38001"}) == date(2024, 1, 1)


def test_earliest_safe_start_falls_back_to_default():
    conn = FakeConn({"com_evolution": [(None,)]})
    assert earliest_safe_start(conn, "zone_trusted", {"38001"}) == DEFAULT_START
    assert earliest_safe_start(conn, "zone_trusted", set()) == DEFAULT_START
