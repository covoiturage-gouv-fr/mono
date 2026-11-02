import {
  array,
  assign,
  boolean,
  enums,
  Infer,
  integer,
  nullable,
  object,
  optional,
  pattern,
  refine,
  size,
  string,
} from "@/lib/superstruct/index.ts";
import { Id, PlainText, Serial } from "@/pdc/providers/superstruct/shared/index.ts";

const TerritoryType = enums(["com", "epci", "aom", "dep", "reg", "country", "custom"]);
const Eligibilite = enums(["tous", "jeunes_18_25", "demandeurs_emploi", "mineurs", "autre"]);
// Chaîne AAAA-MM-JJ (comparable telle quelle) ; `DateOnly` partagé convertit en Date.
// Aller-retour : refuse les dates impossibles (2026-02-31) que Postgres rejetterait en 500.
const IsoDate = refine(pattern(string(), /^\d{4}-\d{2}-\d{2}$/), "iso_date", (v) => {
  const d = new Date(`${v}T00:00:00Z`);
  return (!Number.isNaN(d.getTime()) && d.toISOString().slice(0, 10) === v) || "Date invalide";
});
// Centimes ou nombres de trajets / km : entiers positifs, bornés au int Postgres.
const Count = nullable(size(integer(), 0, 2147483647));
const LongText = refine(size(string(), 0, 2000), "plaintext", (v) => !/[<>]/.test(v) || "Balises interdites");

const fields = {
  territory_type: TerritoryType,
  territory_code: pattern(string(), /^[0-9A-Za-z]{1,20}$/),
  territory_label: PlainText,
  operator_ids: size(array(Serial), 1, 50),
  premiere_campagne: boolean(),
  date_debut: IsoDate,
  date_fin: nullable(IsoDate),
  budget_incitations: Count,
  conducteur_montant_min_par_passager: Count,
  conducteur_montant_max_par_passager: Count,
  conducteur_montant_max_par_mois: Count,
  conducteur_trajets_max_par_mois: Count,
  passager_gratuite: boolean(),
  passager_eligible_gratuite: nullable(Eligibilite),
  passager_reduction_ticket: boolean(),
  passager_eligibilite_reduction: nullable(Eligibilite),
  passager_montant_ticket: Count,
  passager_trajets_max_par_mois: Count,
  zone_sens_des_trajets: nullable(enums(["ET", "OU", "ET/OU"])),
  zone_exclusion: boolean(),
  zone_exclusion_liste: nullable(LongText),
  autre_exclusion: nullable(LongText),
  trajet_longueur_min: Count,
  trajet_longueur_max: Count,
  trajet_classe_preuve: size(array(enums(["A", "B", "C"])), 1, 3),
  lien_page_collectivite: nullable(pattern(size(string(), 0, 500), /^https?:\/\/[^\s<>"]+$/)),
  autres_informations: nullable(LongText),
};

type Fields = Infer<ReturnType<typeof object<typeof fields>>>;

// Mêmes règles croisées que le formulaire app-partners (helpers/campaignCondition.ts).
export function crossRuleError(c: Fields): string | true {
  if (c.date_fin && c.date_fin < c.date_debut) return "date_fin doit suivre date_debut";
  const minP = c.conducteur_montant_min_par_passager;
  const maxP = c.conducteur_montant_max_par_passager;
  if (minP !== null && maxP !== null && minP > maxP) return "montant min par passager supérieur au max";
  if (
    c.trajet_longueur_min !== null && c.trajet_longueur_max !== null && c.trajet_longueur_min > c.trajet_longueur_max
  ) {
    return "longueur min supérieure à la max";
  }
  if (c.passager_gratuite && !c.passager_eligible_gratuite) return "éligibilité à la gratuité requise";
  if (c.passager_reduction_ticket && !c.passager_eligibilite_reduction) return "éligibilité à la réduction requise";
  if (c.zone_exclusion && !c.zone_exclusion_liste?.trim()) return "liste des zones exclues requise";
  return true;
}

export const CreateCampaignCondition = refine(object(fields), "campaign_condition", crossRuleError);
export const UpdateCampaignCondition = refine(
  assign(object(fields), object({ id: Id })),
  "campaign_condition",
  crossRuleError,
);
export const CampaignConditionId = object({ id: Id });
export const CampaignConditions = object({ id: optional(Id) });

export type CreateCampaignCondition = Infer<typeof CreateCampaignCondition>;
export type UpdateCampaignCondition = Infer<typeof UpdateCampaignCondition>;
export type CampaignConditionId = Infer<typeof CampaignConditionId>;
export type CampaignConditions = Infer<typeof CampaignConditions>;
