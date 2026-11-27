import { roleColor, roleDotClass, territoriesCell } from "@/helpers/usersTable";
import { describe, expect, it } from "vitest";

describe("territoriesCell", () => {
  it("rend une cellule vide sans périmètre", () => {
    expect(territoriesCell(undefined)).toEqual({ label: "", extra: 0, all: [] });
    expect(territoriesCell([])).toEqual({ label: "", extra: 0, all: [] });
  });

  it("affiche le seul territoire sans suffixe", () => {
    const cell = territoriesCell([{ territory_id: 1, is_default: true, territory_name: "Lyon" }]);
    expect(cell).toEqual({ label: "Lyon", extra: 0, all: ["Lyon"] });
  });

  it("met le défaut en tête et compte les autres", () => {
    const cell = territoriesCell([
      { territory_id: 1, territory_name: "Lyon" },
      { territory_id: 2, is_default: true, territory_name: "IDFM" },
      { territory_id: 3, territory_name: "Nantes" },
    ]);
    expect(cell).toEqual({ label: "IDFM", extra: 2, all: ["Lyon", "IDFM", "Nantes"] });
  });

  it("se replie sur l'identifiant quand le nom manque", () => {
    const cell = territoriesCell([
      { territory_id: 7, is_default: true },
      { territory_id: 8, territory_name: null },
    ]);
    expect(cell).toEqual({ label: "#7", extra: 1, all: ["#7", "#8"] });
  });

  it("résout un nom manquant via nameOf avant de se replier sur l'identifiant", () => {
    const nameOf = (id: number) => (id === 42 ? "Grand Lyon" : undefined);
    expect(territoriesCell(undefined, 42, nameOf)).toEqual({ label: "Grand Lyon", extra: 0, all: ["Grand Lyon"] });
    expect(territoriesCell([{ territory_id: 9 }], undefined, nameOf)).toEqual({ label: "#9", extra: 0, all: ["#9"] });
  });

  it("se replie sur la colonne territory_id legacy sans scopes", () => {
    expect(territoriesCell(undefined, 42)).toEqual({ label: "#42", extra: 0, all: ["#42"] });
    expect(territoriesCell([], 42)).toEqual({ label: "#42", extra: 0, all: ["#42"] });
  });
});

describe("roleColor", () => {
  it("associe une couleur à chaque famille de rôle", () => {
    expect(roleColor("registry.admin")).toBe("blue");
    expect(roleColor("operator.user")).toBe("orange");
    expect(roleColor("territory.admin")).toBe("green");
  });

  it("ne colore pas les rôles hors famille", () => {
    expect(roleColor("demo")).toBeUndefined();
    expect(roleColor("")).toBeUndefined();
  });
});

describe("roleDotClass", () => {
  it("pastille pleine pour un admin, cerclée pour un utilisateur", () => {
    expect(roleDotClass("territory.admin")).toBe("role-pill__dot role-pill__dot--green");
    expect(roleDotClass("operator.user")).toBe("role-pill__dot role-pill__dot--orange role-pill__dot--outline");
  });

  it("gris cerclé pour un rôle inconnu", () => {
    expect(roleDotClass("demo")).toBe("role-pill__dot role-pill__dot--grey role-pill__dot--outline");
  });
});
