import { operatorSiret, siretAfterRoleChange, siretAfterScopesChange, suggestSiret, userFormSchema } from "@/helpers/userForm";
import { describe, expect, it } from "vitest";

const valid = {
  firstname: "Camille",
  lastname: "Martin",
  email: "camille@example.org",
  role: "territory.user",
  operator_id: null,
  territory_id: 12,
  login_siret: "13002526500013",
  scopes: [{ territory_id: 12, is_default: true }],
};

const fieldErrors = (row: Record<string, unknown>) => {
  const r = userFormSchema.safeParse(row);
  return r.success ? {} : r.error.flatten().fieldErrors;
};

describe("userFormSchema", () => {
  it("accepte un compte territoire complet", () => {
    expect(userFormSchema.safeParse(valid).success).toBe(true);
  });

  it("rogne les noms et normalise l'email", () => {
    const r = userFormSchema.parse({ ...valid, firstname: "  Camille ", email: " Camille@Example.ORG " });
    expect(r.firstname).toBe("Camille");
    expect(r.email).toBe("camille@example.org");
  });

  it("refuse les blancs, les chevrons et plus de 256 caractères", () => {
    expect(fieldErrors({ ...valid, firstname: "     " }).firstname).toBeDefined();
    expect(fieldErrors({ ...valid, lastname: "<script>" }).lastname).toEqual([
      "Les caractères < et > ne sont pas autorisés",
    ]);
    expect(fieldErrors({ ...valid, lastname: "a".repeat(257) }).lastname).toBeDefined();
  });

  it("accepte un SIRET de 14 chiffres ou null, jamais une chaîne vide", () => {
    expect(fieldErrors({ ...valid, login_siret: "12345678900012" }).login_siret).toBeUndefined();
    expect(fieldErrors({ ...valid, role: "registry.admin", login_siret: null }).login_siret).toBeUndefined();
    expect(fieldErrors({ ...valid, login_siret: "" }).login_siret).toBeDefined();
    expect(fieldErrors({ ...valid, login_siret: "123456789" }).login_siret).toBeDefined();
  });

  it("exige le SIRET de connexion pour les rôles opérateur et territoire quand le champ est affiché", () => {
    const required = "Le SIRET de connexion ProConnect est obligatoire (14 chiffres)";
    expect(fieldErrors({ ...valid, login_siret: null }).login_siret).toEqual([required]);
    expect(fieldErrors({ ...valid, role: "territory.admin", login_siret: null }).login_siret).toEqual([required]);
    expect(
      fieldErrors({ ...valid, role: "operator.user", operator_id: 3, login_siret: null }).login_siret,
    ).toEqual([required]);
    expect(
      fieldErrors({ ...valid, role: "operator.admin", operator_id: 3, login_siret: "12345678900012" }).login_siret,
    ).toBeUndefined();
  });

  it("laisse le SIRET optionnel pour le registre, et absent quand le champ est masqué", () => {
    expect(fieldErrors({ ...valid, role: "registry.admin", scopes: undefined, login_siret: null }).login_siret)
      .toBeUndefined();
    // Hors registry.admin le champ n'est pas envoyé : l'API retombe sur le SIRET du territoire ou de l'opérateur.
    expect(fieldErrors({ ...valid, login_siret: undefined }).login_siret).toBeUndefined();
  });

  it("exige un opérateur pour un rôle opérateur", () => {
    expect(fieldErrors({ ...valid, role: "operator.user", operator_id: null }).operator_id).toEqual([
      "Sélectionnez un opérateur",
    ]);
    expect(fieldErrors({ ...valid, role: "operator.user", operator_id: 3 }).operator_id).toBeUndefined();
  });

  // L'API refuse les clés inconnues : les champs d'affichage de la liste ne doivent pas repartir.
  it("retire territory_name et siret des périmètres avant envoi", () => {
    const r = userFormSchema.parse({
      ...valid,
      scopes: [{ territory_id: 12, is_default: true, territory_name: "IDFM", siret: "13002526500013" }],
    });
    expect(r.scopes).toEqual([{ territory_id: 12, is_default: true }]);
  });

  it("exige un périmètre pour un rôle territoire quand les périmètres sont édités", () => {
    expect(fieldErrors({ ...valid, scopes: [] }).scopes).toEqual(["Ajoutez au moins un périmètre"]);
    // territory.admin n'édite pas les périmètres : scopes absent du corps.
    expect(fieldErrors({ ...valid, scopes: undefined }).scopes).toBeUndefined();
  });

  it("refuse un rôle inconnu", () => {
    expect(fieldErrors({ ...valid, role: "superuser" }).role).toBeDefined();
  });
});

describe("suggestSiret", () => {
  it("prend le SIRET du périmètre par défaut, sinon du premier", () => {
    expect(
      suggestSiret([
        { territory_id: 1, siret: "11111111100011" },
        { territory_id: 2, is_default: true, siret: "22222222200022" },
      ]),
    ).toBe("22222222200022");
    expect(suggestSiret([{ territory_id: 1, siret: "11111111100011" }])).toBe("11111111100011");
  });

  it("renvoie null, jamais une chaîne vide, quand il n'y a rien à suggérer", () => {
    expect(suggestSiret([])).toBeNull();
    expect(suggestSiret([{ territory_id: 1, is_default: true, siret: null }])).toBeNull();
    expect(suggestSiret([{ territory_id: 1, is_default: true }])).toBeNull();
  });
});

describe("siretAfterScopesChange", () => {
  const scopes = [{ territory_id: 1, is_default: true, siret: "11111111100011" }];

  it("remplit un SIRET vide avec celui du périmètre par défaut", () => {
    expect(siretAfterScopesChange(null, scopes)).toBe("11111111100011");
    expect(siretAfterScopesChange(undefined, scopes)).toBe("11111111100011");
    expect(siretAfterScopesChange("", scopes)).toBe("11111111100011");
  });

  it("n'écrase jamais une valeur existante", () => {
    expect(siretAfterScopesChange("99999999900099", scopes)).toBe("99999999900099");
  });

  it("reste vide si le périmètre n'a pas de SIRET", () => {
    expect(siretAfterScopesChange(null, [{ territory_id: 1, is_default: true }])).toBeNull();
  });
});

describe("operatorSiret", () => {
  const operators = [
    { id: 3, name: "Karos", siret: "11111111100011" },
    { id: 4, name: "Sans SIRET", siret: "" },
  ];

  it("renvoie le SIRET de l'opérateur choisi", () => {
    expect(operatorSiret(operators, 3)).toBe("11111111100011");
  });

  it("renvoie null pour un opérateur inconnu, sans SIRET ou absent", () => {
    expect(operatorSiret(operators, 99)).toBeNull();
    expect(operatorSiret(operators, 4)).toBeNull();
    expect(operatorSiret(operators, null)).toBeNull();
  });
});

describe("siretAfterRoleChange", () => {
  const scopes = [{ territory_id: 1, is_default: true, siret: "11111111100011" }];

  it("garde le SIRET quand la famille de rôle ne change pas", () => {
    expect(siretAfterRoleChange("operator.user", "operator.admin", "22222222200022", [])).toBe("22222222200022");
  });

  it("abandonne le SIRET de l'ancienne famille", () => {
    expect(siretAfterRoleChange("operator.admin", "registry.admin", "22222222200022", [])).toBeNull();
    expect(siretAfterRoleChange("territory.admin", "operator.admin", "11111111100011", scopes)).toBeNull();
  });

  it("repart du périmètre par défaut en passant à un rôle territoire", () => {
    expect(siretAfterRoleChange("operator.admin", "territory.admin", "22222222200022", scopes)).toBe("11111111100011");
    expect(siretAfterRoleChange("operator.admin", "territory.admin", "22222222200022", [])).toBeNull();
  });
});
