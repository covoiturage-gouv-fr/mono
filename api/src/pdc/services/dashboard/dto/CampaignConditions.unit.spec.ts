import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { Struct, validate } from "@/lib/superstruct/index.ts";
import { CreateCampaignCondition, UpdateCampaignCondition } from "./CampaignConditions.ts";

describe("dto/CampaignConditions", () => {
  // deno-lint-ignore no-explicit-any
  const failures = (payload: unknown, struct: Struct<any, any> = CreateCampaignCondition): string[] => {
    const [err] = validate(payload, struct, { coerce: true });
    return err ? err.failures().map((f) => `${f.path.join(".")} : ${f.message}`) : [];
  };

  // Montants en centimes, distances en km.
  const base = {
    territory_type: "custom",
    territory_code: "36709",
    territory_label: "PETR Alsace du Nord",
    operator_ids: [3],
    premiere_campagne: false,
    date_debut: "2026-01-01",
    date_fin: "2026-12-31",
    budget_incitations: 5000000,
    conducteur_montant_min_par_passager: 150,
    conducteur_montant_max_par_passager: 300,
    conducteur_montant_max_par_mois: 15000,
    conducteur_trajets_max_par_mois: 60,
    passager_gratuite: false,
    passager_eligible_gratuite: null,
    passager_reduction_ticket: false,
    passager_eligibilite_reduction: null,
    passager_montant_ticket: null,
    passager_trajets_max_par_mois: null,
    zone_sens_des_trajets: "OU",
    zone_exclusion: false,
    zone_exclusion_liste: null,
    autre_exclusion: null,
    trajet_longueur_min: 5,
    trajet_longueur_max: 80,
    trajet_classe_preuve: ["B", "C"],
    lien_page_collectivite: "https://example.org/covoiturage",
    autres_informations: null,
  };

  it("accepte une campagne complète", () => {
    assertEquals(failures(base), []);
    assertEquals(failures({ ...base, id: 12 }, UpdateCampaignCondition), []);
  });

  it("accepte une campagne sans date de fin ni montants", () => {
    assertEquals(
      failures({ ...base, date_fin: null, budget_incitations: null, conducteur_montant_min_par_passager: null }),
      [],
    );
  });

  it("refuse un operator_id fourni par le client", () => {
    assertEquals(failures({ ...base, operator_id: 3 }).length > 0, true);
  });

  it("refuse un type de territoire inconnu", () => {
    assertEquals(failures({ ...base, territory_type: "arr" }).length > 0, true);
  });

  it("exige au moins un opérateur", () => {
    assertEquals(failures({ ...base, operator_ids: [] }).length > 0, true);
  });

  it("refuse une date impossible et un montant au-delà d'un int Postgres", () => {
    assertEquals(failures({ ...base, date_debut: "2026-02-31" }).length > 0, true);
    assertEquals(failures({ ...base, budget_incitations: 2147483648 }).length > 0, true);
    assertEquals(failures({ ...base, budget_incitations: 2147483647 }), []);
  });

  it("refuse un montant négatif ou décimal (centimes entiers)", () => {
    assertEquals(failures({ ...base, budget_incitations: -1 }).length > 0, true);
    assertEquals(failures({ ...base, budget_incitations: 10.5 }).length > 0, true);
  });

  it("refuse une fin avant le début", () => {
    assertEquals(failures({ ...base, date_fin: "2025-12-31" }).length > 0, true);
  });

  it("refuse un montant min conducteur supérieur au max", () => {
    assertEquals(
      failures({ ...base, conducteur_montant_min_par_passager: 400, conducteur_montant_max_par_passager: 300 })
        .length > 0,
      true,
    );
  });

  it("refuse une longueur min supérieure à la max", () => {
    assertEquals(failures({ ...base, trajet_longueur_min: 90 }).length > 0, true);
  });

  it("exige l'éligibilité quand la gratuité ou la réduction est cochée", () => {
    assertEquals(failures({ ...base, passager_gratuite: true }).length > 0, true);
    assertEquals(failures({ ...base, passager_gratuite: true, passager_eligible_gratuite: "tous" }), []);
    assertEquals(failures({ ...base, passager_reduction_ticket: true }).length > 0, true);
  });

  it("exige la liste des zones quand une exclusion est cochée", () => {
    assertEquals(failures({ ...base, zone_exclusion: true }).length > 0, true);
    assertEquals(failures({ ...base, zone_exclusion: true, zone_exclusion_liste: "Strasbourg" }), []);
  });

  it("exige au moins une classe de preuve valide", () => {
    assertEquals(failures({ ...base, trajet_classe_preuve: [] }).length > 0, true);
    assertEquals(failures({ ...base, trajet_classe_preuve: ["D"] }).length > 0, true);
  });

  it("refuse un lien qui n'est pas en http(s)", () => {
    assertEquals(failures({ ...base, lien_page_collectivite: "javascript:alert(1)" }).length > 0, true);
  });

  it("refuse les balises dans les champs texte", () => {
    assertEquals(failures({ ...base, autres_informations: "<script>" }).length > 0, true);
  });
});
