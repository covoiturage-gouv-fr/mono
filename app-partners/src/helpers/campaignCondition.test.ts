import {
  canCreateCampaign,
  canEditCampaign,
  emptyCampaignCondition,
  type CampaignConditionForm,
  campaignConditionSchema,
  fromApiRow,
  toApiPayload,
} from "@/helpers/campaignCondition";
import { describe, expect, it } from "vitest";

const valid = {
  ...emptyCampaignCondition(),
  territory: { type: "custom", code: "36709", label: "PETR Alsace du Nord" },
  operator_ids: [3],
  date_debut: "2026-01-01",
  date_fin: "2026-12-31",
  trajet_classe_preuve: ["C"],
};

const fieldErrors = (row: Record<string, unknown>) => {
  const r = campaignConditionSchema.safeParse(row);
  return r.success ? {} : r.error.flatten().fieldErrors;
};

describe("campaignConditionSchema", () => {
  it("accepte une campagne complète", () => {
    expect(campaignConditionSchema.safeParse(valid).success).toBe(true);
  });

  it("accepte une campagne sans date de fin", () => {
    expect(campaignConditionSchema.safeParse({ ...valid, date_fin: null }).success).toBe(true);
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
    expect(campaignConditionSchema.safeParse({ ...valid, date_fin: valid.date_debut }).success).toBe(true);
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
      campaignConditionSchema.safeParse({ ...valid, passager_gratuite: true, passager_eligible_gratuite: "tous" })
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
      campaignConditionSchema.safeParse({ ...valid, lien_page_collectivite: "https://example.org/covoit" }).success,
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
  } as CampaignConditionForm;

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

  it("relit une ligne de l'API dans le format du formulaire", () => {
    const meta = { id: 4, status: "draft" as const, created_by_operator_id: 3, created_by_territory_id: null };
    expect(fromApiRow({ ...toApiPayload(form), ...meta })).toEqual({ ...form, ...meta });
  });
});
