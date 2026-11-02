import { type PerimeterType, perimeterTypes } from "@/interfaces/searchInterface";
import { z } from "zod";

export const eligibilites = ["tous", "jeunes_18_25", "demandeurs_emploi", "mineurs", "autre"] as const;
export const eligibiliteLabels: Record<(typeof eligibilites)[number], string> = {
  tous: "Tous",
  jeunes_18_25: "Jeunes 18-25 ans",
  demandeurs_emploi: "Demandeurs d'emploi",
  mineurs: "Mineurs",
  autre: "Autre",
};
export const sensTrajets = ["ET", "OU", "ET/OU"] as const;
export const sensTrajetsLabels: Record<(typeof sensTrajets)[number], string> = {
  ET: "Origine ET destination dans la zone",
  OU: "Origine OU destination dans la zone",
  "ET/OU": "Les deux sont acceptés",
};
export const classesPreuve = ["A", "B", "C"] as const;
export const campaignStatuses = ["draft", "validated"] as const;

const montant = z.number().min(0, { message: "Le montant doit être positif" }).nullable();
const entier = z.number().int({ message: "Nombre entier attendu" }).min(0, { message: "Doit être positif" }).nullable();
const texte = (max: number) =>
  z
    .string()
    .trim()
    .max(max, { message: `${max} caractères maximum` })
    .regex(/^[^<>]*$/, { message: "Les caractères < et > ne sont pas autorisés" });

// Montants en euros côté formulaire ; l'API les stockera en centimes.
export const campaignConditionSchema = z
  .object({
    territory: z.object(
      { type: z.enum(perimeterTypes), code: z.string().min(1), label: z.string() },
      { message: "Sélectionnez un territoire" },
    ),
    operator_ids: z.array(z.number()).min(1, { message: "Sélectionnez au moins un opérateur" }),
    premiere_campagne: z.boolean(),
    date_debut: z.string().date("Date de début requise"),
    date_fin: z.string().date("Date invalide").nullable(),
    budget_incitations: montant,
    conducteur_montant_min_par_passager: montant,
    conducteur_montant_max_par_passager: montant,
    conducteur_montant_max_par_mois: montant,
    conducteur_trajets_max_par_mois: entier,
    passager_gratuite: z.boolean(),
    passager_eligible_gratuite: z.enum(eligibilites).nullable(),
    passager_reduction_ticket: z.boolean(),
    passager_eligibilite_reduction: z.enum(eligibilites).nullable(),
    passager_montant_ticket: montant,
    passager_trajets_max_par_mois: entier,
    zone_sens_des_trajets: z.enum(sensTrajets).nullable(),
    zone_exclusion: z.boolean(),
    zone_exclusion_liste: texte(2000).nullable(),
    autre_exclusion: texte(2000).nullable(),
    trajet_longueur_min: entier,
    trajet_longueur_max: entier,
    trajet_classe_preuve: z.array(z.enum(classesPreuve)).min(1, { message: "Cochez au moins une classe de preuve" }),
    lien_page_collectivite: z
      .string()
      .trim()
      .url({ message: "Lien invalide" })
      .regex(/^https?:\/\//, { message: "Le lien doit commencer par http:// ou https://" })
      .nullable(),
    autres_informations: texte(2000).nullable(),
  })
  .superRefine((row, ctx) => {
    const issue = (path: string, message: string) =>
      ctx.addIssue({ code: z.ZodIssueCode.custom, path: [path], message });
    if (row.date_fin && row.date_fin < row.date_debut) {
      issue("date_fin", "La date de fin doit suivre la date de début");
    }
    const min = row.conducteur_montant_min_par_passager;
    const max = row.conducteur_montant_max_par_passager;
    if (min !== null && max !== null && min > max) {
      issue("conducteur_montant_min_par_passager", "Le minimum dépasse le maximum");
    }
    if (
      row.trajet_longueur_min !== null &&
      row.trajet_longueur_max !== null &&
      row.trajet_longueur_min > row.trajet_longueur_max
    ) {
      issue("trajet_longueur_min", "La longueur minimale dépasse la maximale");
    }
    if (row.passager_gratuite && !row.passager_eligible_gratuite) {
      issue("passager_eligible_gratuite", "Précisez qui bénéficie de la gratuité");
    }
    if (row.passager_reduction_ticket && !row.passager_eligibilite_reduction) {
      issue("passager_eligibilite_reduction", "Précisez qui bénéficie de la réduction");
    }
    if (row.zone_exclusion && !row.zone_exclusion_liste?.trim()) {
      issue("zone_exclusion_liste", "Listez les zones exclues");
    }
  });

export type CampaignConditionForm = z.input<typeof campaignConditionSchema>;

export interface CampaignCondition extends CampaignConditionForm {
  id: number;
  status: (typeof campaignStatuses)[number];
  created_by_operator_id: number | null;
  created_by_territory_id: number | null;
}

export const emptyCampaignCondition = (): Omit<CampaignConditionForm, "territory"> & {
  territory: CampaignConditionForm["territory"] | null;
} => ({
  territory: null,
  operator_ids: [],
  premiere_campagne: false,
  date_debut: "",
  date_fin: null,
  budget_incitations: null,
  conducteur_montant_min_par_passager: null,
  conducteur_montant_max_par_passager: null,
  conducteur_montant_max_par_mois: null,
  conducteur_trajets_max_par_mois: null,
  passager_gratuite: false,
  passager_eligible_gratuite: null,
  passager_reduction_ticket: false,
  passager_eligibilite_reduction: null,
  passager_montant_ticket: null,
  passager_trajets_max_par_mois: null,
  zone_sens_des_trajets: null,
  zone_exclusion: false,
  zone_exclusion_liste: null,
  autre_exclusion: null,
  trajet_longueur_min: null,
  trajet_longueur_max: null,
  trajet_classe_preuve: [],
  lien_page_collectivite: null,
  autres_informations: null,
});

// Mêmes règles que l'API (campaignConditionAccess.ts) : seuls les admins écrivent ;
// un opérateur modifie ses campagnes mono-opérateur, les multi-opérateurs sont saisies par l'équipe RPC.
export const canCreateCampaign = (role: string) =>
  role === "registry.admin" || role === "operator.admin" || role === "territory.admin";

export const canEditCampaign = (
  campaign: Pick<CampaignCondition, "operator_ids" | "created_by_operator_id" | "created_by_territory_id">,
  user: { role: string; operator_id: number | null; territory_id?: number | null },
) => {
  if (user.role === "registry.admin") return true;
  // L'API ne liste à un territoire que les campagnes qu'il a saisies ou qui portent sur lui : il a la main dessus.
  if (user.role === "territory.admin") return user.territory_id != null;
  return (
    user.role === "operator.admin" &&
    user.operator_id !== null &&
    campaign.created_by_operator_id === user.operator_id &&
    campaign.operator_ids.length === 1 &&
    campaign.operator_ids[0] === user.operator_id
  );
};

// L'API stocke les montants en centimes et le territoire à plat.
const MONTANTS = [
  "budget_incitations",
  "conducteur_montant_min_par_passager",
  "conducteur_montant_max_par_passager",
  "conducteur_montant_max_par_mois",
  "passager_montant_ticket",
] as const;

interface ApiTerritory {
  territory_type: PerimeterType;
  territory_code: string;
  territory_label: string;
}
export type CampaignConditionPayload = Omit<CampaignConditionForm, "territory"> & ApiTerritory;
export type CampaignConditionApiRow = CampaignConditionPayload & Omit<CampaignCondition, keyof CampaignConditionForm>;

export const toApiPayload = ({ territory, ...rest }: CampaignConditionForm): CampaignConditionPayload => {
  const payload: CampaignConditionPayload = {
    ...rest,
    territory_type: territory.type,
    territory_code: territory.code,
    territory_label: territory.label,
  };
  for (const k of MONTANTS) {
    const v = payload[k];
    payload[k] = v === null || v === undefined ? null : Math.round(v * 100);
  }
  return payload;
};

export const fromApiRow = ({
  territory_type,
  territory_code,
  territory_label,
  ...rest
}: CampaignConditionApiRow): CampaignCondition => {
  const row: CampaignCondition = {
    ...rest,
    territory: { type: territory_type, code: territory_code, label: territory_label },
  };
  for (const k of MONTANTS) {
    const v = row[k];
    row[k] = v === null || v === undefined ? null : v / 100;
  }
  return row;
};
