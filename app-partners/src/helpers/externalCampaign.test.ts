import {
  campaignListScope,
  crossRuleIssues,
  canCreateCampaign,
  canEditCampaign,
  emptyExternalCampaign,
  type ExternalCampaignForm,
  externalCampaignSchema,
  fromApiRow,
  toApiPayload,
  visibleErrors,
} from "@/helpers/externalCampaign";
import { describe, expect, it } from "vitest";

const valid = {
  ...emptyExternalCampaign(),
  territory: { type: "custom", code: "36709", label: "PETR Alsace du Nord" },
  operator_ids: [3],
  date_debut: "2026-01-01",
  date_fin: "2026-12-31",
  trajet_classe_preuve: ["C"],
};

const fieldErrors = (row: Record<string, unknown>) => {
  const r = externalCampaignSchema.safeParse(row);
  return r.success ? {} : r.error.flatten().fieldErrors;
};

describe("externalCampaignSchema", () => {
  it("accepte une campagne complète", () => {
    expect(externalCampaignSchema.safeParse(valid).success).toBe(true);
  });

  it("accepte une campagne sans date de fin", () => {
    expect(externalCampaignSchema.safeParse({ ...valid, date_fin: null }).success).toBe(true);
  });

  it("exige un territoire", () => {
    expect(fieldErrors({ ...valid, territory: null })).toHaveProperty("territory");
  });

  it("exige au moins un opérateur", () => {
    expect(fieldErrors({ ...valid, operator_ids: [] })).toHaveProperty("operator_ids");
  });

  it("refuse une fin avant le début", () => {
    expect(fieldErrors({ ...valid, date_fin: "2025-12-31" })).toHaveProperty("date_fin");
  });

  it("accepte une campagne d'une seule journée", () => {
    expect(externalCampaignSchema.safeParse({ ...valid, date_fin: valid.date_debut }).success).toBe(true);
  });

  it("refuse un montant min conducteur supérieur au max", () => {
    const row = { ...valid, conducteur_montant_min_par_passager: 3, conducteur_montant_max_par_passager: 2 };
    expect(fieldErrors(row)).toHaveProperty("conducteur_montant_min_par_passager");
  });

  it("refuse une longueur min supérieure à la max", () => {
    expect(fieldErrors({ ...valid, trajet_longueur_min: 80, trajet_longueur_max: 5 })).toHaveProperty(
      "trajet_longueur_min",
    );
  });

  it("refuse un montant négatif", () => {
    expect(fieldErrors({ ...valid, budget_incitations: -1 })).toHaveProperty("budget_incitations");
  });

  it("exige l'éligibilité quand la gratuité passager est cochée", () => {
    expect(fieldErrors({ ...valid, passager_gratuite: true })).toHaveProperty("passager_eligible_gratuite");
    expect(
      externalCampaignSchema.safeParse({ ...valid, passager_gratuite: true, passager_eligible_gratuite: "tous" })
        .success,
    ).toBe(true);
  });

  it("exige l'éligibilité quand la réduction de ticket est cochée", () => {
    expect(fieldErrors({ ...valid, passager_reduction_ticket: true })).toHaveProperty("passager_eligibilite_reduction");
  });

  it("exige la liste des zones quand une exclusion est cochée", () => {
    expect(fieldErrors({ ...valid, zone_exclusion: true })).toHaveProperty("zone_exclusion_liste");
    expect(fieldErrors({ ...valid, zone_exclusion: true, zone_exclusion_liste: "   " })).toHaveProperty(
      "zone_exclusion_liste",
    );
  });

  it("exige au moins une classe de preuve", () => {
    expect(fieldErrors({ ...valid, trajet_classe_preuve: [] })).toHaveProperty("trajet_classe_preuve");
  });

  it("refuse un lien qui n'est pas une URL http(s)", () => {
    expect(fieldErrors({ ...valid, lien_page_collectivite: "javascript:alert(1)" })).toHaveProperty(
      "lien_page_collectivite",
    );
    expect(
      externalCampaignSchema.safeParse({ ...valid, lien_page_collectivite: "https://example.org/covoit" }).success,
    ).toBe(true);
  });
});

describe("canEditCampaign", () => {
  const mono = { operator_ids: [3], created_by_operator_id: 3, created_by_territory_id: null };
  const multi = { operator_ids: [3, 5], created_by_operator_id: null, created_by_territory_id: null };
  const byTerritory = { operator_ids: [3, 5], created_by_operator_id: null, created_by_territory_id: 7 };
  const territory = (role: string, territory_id: number) => ({ role, operator_id: null, territory_id });

  it("laisse les admins RPC tout modifier, pas les registry.user", () => {
    expect(canEditCampaign(multi, { role: "registry.user", operator_id: null })).toBe(false);
  });

  it("laisse l'équipe RPC tout modifier", () => {
    expect(canEditCampaign(multi, { role: "registry.admin", operator_id: null })).toBe(true);
  });

  it("laisse un opérateur modifier sa campagne mono-opérateur", () => {
    expect(canEditCampaign(mono, { role: "operator.admin", operator_id: 3 })).toBe(true);
  });

  it("refuse à un opérateur une campagne multi-opérateurs où il est listé", () => {
    expect(canEditCampaign(multi, { role: "operator.admin", operator_id: 3 })).toBe(false);
  });

  it("refuse à un opérateur la campagne d'un autre", () => {
    expect(canEditCampaign(mono, { role: "operator.admin", operator_id: 5 })).toBe(false);
  });

  it("laisse un admin territoire modifier les campagnes que l'API lui liste, quel qu'en soit l'auteur", () => {
    expect(canEditCampaign(byTerritory, territory("territory.admin", 7))).toBe(true);
    expect(canEditCampaign(mono, territory("territory.admin", 7))).toBe(true);
    expect(canEditCampaign(byTerritory, territory("territory.user", 7))).toBe(false);
  });

  it("met les comptes operator.user en lecture seule", () => {
    expect(canEditCampaign(mono, { role: "operator.user", operator_id: 3 })).toBe(false);
  });
});

describe("canCreateCampaign", () => {
  it("réserve la saisie aux admins RPC et opérateur", () => {
    expect(canCreateCampaign("registry.admin")).toBe(true);
    expect(canCreateCampaign("operator.admin")).toBe(true);
    expect(canCreateCampaign("territory.admin")).toBe(true);
    expect(canCreateCampaign("territory.user")).toBe(false);
    expect(canCreateCampaign("operator.user")).toBe(false);
    expect(canCreateCampaign("registry.user")).toBe(false);
  });
});

describe("conversion API", () => {
  const form = {
    ...valid,
    budget_incitations: 50000,
    conducteur_montant_min_par_passager: 1.15,
  } as ExternalCampaignForm;

  it("aplatit le territoire et convertit les euros en centimes entiers", () => {
    const payload = toApiPayload(form);
    expect(payload).toMatchObject({
      territory_type: "custom",
      territory_code: "36709",
      territory_label: "PETR Alsace du Nord",
      budget_incitations: 5000000,
      conducteur_montant_min_par_passager: 115,
      passager_montant_ticket: null,
      trajet_longueur_max: null,
    });
    expect(payload).not.toHaveProperty("territory");
  });

  it("ne renvoie pas les champs posés par le serveur", () => {
    const row = { ...form, status: "draft", created_by_operator_id: 3, created_at: "2026-01-01", validated_at: null };
    const payload = toApiPayload(row);
    for (const k of ["status", "created_by_operator_id", "created_at", "validated_at"]) {
      expect(payload).not.toHaveProperty(k);
    }
  });

  it("relit une ligne de l'API dans le format du formulaire", () => {
    const meta = { id: 4, status: "draft" as const, created_by_operator_id: 3, created_by_territory_id: null };
    expect(fromApiRow({ ...toApiPayload(form), ...meta })).toEqual({ ...form, ...meta });
  });
});

describe("campaignListScope", () => {
  const admin = { operator_id: null, territory_id: null };

  it("hors simulation, la liste suit la session sans filtre", () => {
    expect(campaignListScope(admin, undefined)).toEqual({ params: {}, pending: false });
    expect(campaignListScope({ operator_id: 3, territory_id: null }, undefined)).toEqual({
      params: {},
      pending: false,
    });
  });

  it("en simulation, filtre sur l'opérateur ou le territoire choisi", () => {
    expect(campaignListScope({ ...admin, operator_id: 3 }, "operator")).toEqual({
      params: { operator_id: 3 },
      pending: false,
    });
    expect(campaignListScope({ ...admin, territory_id: 7 }, "territory")).toEqual({
      params: { territory_id: 7 },
      pending: false,
    });
  });

  it("en simulation sans choix, n'affiche rien plutôt que toutes les campagnes", () => {
    expect(campaignListScope(admin, "operator").pending).toBe(true);
    expect(campaignListScope({ ...admin, operator_id: 3 }, "territory").pending).toBe(true);
  });
});

describe("visibleErrors", () => {
  const errors = {
    date_debut: ["Date de début requise"],
    date_fin: ["La date de fin doit suivre la date de début"],
    budget_incitations: ["Le montant doit être positif"],
  };

  it("n'affiche rien tant que rien n'est touché", () => {
    expect(visibleErrors(errors, new Set(), false)).toEqual({});
  });

  it("affiche l'erreur d'un champ touché seulement", () => {
    expect(visibleErrors(errors, new Set(["budget_incitations"]), false)).toEqual({
      budget_incitations: "Le montant doit être positif",
    });
  });

  it("révèle une règle croisée quand le champ lié est touché", () => {
    expect(visibleErrors({ date_fin: errors.date_fin }, new Set(["date_debut"]), false)).toEqual({
      date_fin: "La date de fin doit suivre la date de début",
    });
  });

  it("n'affiche pas l'erreur d'un champ lié dans l'autre sens", () => {
    expect(visibleErrors({ date_debut: errors.date_debut }, new Set(["date_fin"]), false)).toEqual({});
  });

  it("affiche tout au submit", () => {
    expect(Object.keys(visibleErrors(errors, new Set(), true))).toEqual([
      "date_debut",
      "date_fin",
      "budget_incitations",
    ]);
  });

  it("couvre chaque règle croisée du schéma", () => {
    const touched = new Set(["passager_gratuite", "zone_exclusion", "conducteur_montant_max_par_passager"]);
    const row = {
      ...valid,
      passager_gratuite: true,
      zone_exclusion: true,
      conducteur_montant_min_par_passager: 5,
      conducteur_montant_max_par_passager: 1,
    };
    expect(Object.keys(visibleErrors(fieldErrors(row), touched, false)).sort()).toEqual([
      "conducteur_montant_min_par_passager",
      "passager_eligible_gratuite",
      "zone_exclusion_liste",
    ]);
  });

  it("révèle date_fin avec un territoire vide quand date_debut est touché", () => {
    const draft = { ...emptyExternalCampaign(), date_debut: "2026-05-01", date_fin: "2026-04-01" };
    const errors: Record<string, string[]> = {};
    for (const { path, message } of crossRuleIssues(draft)) errors[path] = [message];
    expect(visibleErrors(errors, new Set(["date_debut"]), false)).toEqual({
      date_fin: "La date de fin doit suivre la date de début",
    });
  });
});

describe("crossRuleIssues", () => {
  it("tolère null, NaN et chaînes vides", () => {
    expect(
      crossRuleIssues({
        date_debut: "",
        date_fin: "",
        conducteur_montant_min_par_passager: NaN,
        conducteur_montant_max_par_passager: 1,
        trajet_longueur_min: null,
        trajet_longueur_max: 2,
        zone_exclusion: false,
      }),
    ).toEqual([]);
  });
});
