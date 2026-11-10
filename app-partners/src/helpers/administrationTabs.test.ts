import { tabIdFromPathname, visibleTabIds } from "@/helpers/administrationTabs";
import { describe, expect, it } from "vitest";

describe("tabIdFromPathname", () => {
  it.each([
    ["/administration/profil/", "profil"],
    ["/administration/profil", "profil"],
    ["/administration/cles-api/", "cles-api"],
    ["/administration/campagnes/", "campagnes"],
    ["/administration/campagnes/declarer/", "campagnes"],
    ["/administration/campagnes/modifier/", "campagnes"],
  ])("%s -> %s", (path, id) => expect(tabIdFromPathname(path)).toBe(id));

  it("ignore les chemins hors onglets", () => {
    expect(tabIdFromPathname("/administration/")).toBeUndefined();
    expect(tabIdFromPathname("/administration/campagnes-autre/")).toBeUndefined();
  });
});

describe("visibleTabIds", () => {
  const registry = { role: "registry.admin", operator_id: null, territory_id: null };

  it("équipe RPC : tous les onglets sauf clés d'API", () => {
    expect(visibleTabIds(registry, false, true)).toEqual([
      "profil",
      "utilisateurs",
      "operateurs",
      "territoires",
      "campagnes",
    ]);
  });

  it("équipe RPC en simulation : plus d'opérateurs ni de territoires", () => {
    expect(visibleTabIds({ ...registry, operator_id: 3 }, true, true)).toEqual([
      "profil",
      "utilisateurs",
      "cles-api",
      "campagnes",
    ]);
  });

  it("compte opérateur non admin : profil et campagnes", () => {
    expect(visibleTabIds({ role: "operator.user", operator_id: 3, territory_id: null }, false, true)).toEqual([
      "profil",
      "campagnes",
    ]);
  });

  it("flag campagnes éteint : aucun onglet campagnes, même pour l'équipe RPC", () => {
    expect(visibleTabIds(registry, false, false)).not.toContain("campagnes");
    expect(visibleTabIds({ role: "operator.user", operator_id: 3, territory_id: null }, false, false)).toEqual([
      "profil",
    ]);
  });

  it("sans utilisateur : profil seul", () => {
    expect(visibleTabIds(undefined, false)).toEqual(["profil"]);
  });
});
