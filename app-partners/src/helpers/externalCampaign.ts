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

type CrossRuleDraft = Partial<Record<string, unknown>>;

const isNum = (v: unknown): v is number => typeof v === "number" && !Number.isNaN(v);

// Règles croisées tolérantes aux brouillons incomplets, évaluables même si le reste du schéma échoue.
export const crossRuleIssues = (row: CrossRuleDraft): { path: string; message: string }[] => {
  const issues: { path: string; message: string }[] = [];
  const issue = (path: string, message: string) => issues.push({ path, message });
  const { date_debut, date_fin } = row;
  if (typeof date_fin === "string" && date_fin && typeof date_debut === "string" && date_fin < date_debut) {
    issue("date_fin", "La date de fin doit suivre la date de début");
  }
  const min = row.conducteur_montant_min_par_passager;
  const max = row.conducteur_montant_max_par_passager;
  if (isNum(min) && isNum(max) && min > max) {
    issue("conducteur_montant_min_par_passager", "Le minimum dépasse le maximum");
  }
  if (
    isNum(row.trajet_longueur_min) &&
    isNum(row.trajet_longueur_max) &&
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
  if (row.zone_exclusion && !(typeof row.zone_exclusion_liste === "string" && row.zone_exclusion_liste.trim())) {
    issue("zone_exclusion_liste", "Listez les zones exclues");
  }
  return issues;
};

// Montants en euros côté formulaire ; l'API les stockera en centimes.
export const externalCampaignSchema = z
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
    for (const { path, message } of crossRuleIssues(row)) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, path: [path], message });
    }
  });

// Champs dont le « touché » révèle aussi l'erreur d'une règle croisée portée par la clé.
export const linkedFields: Record<string, string[]> = {
  date_fin: ["date_debut"],
  conducteur_montant_min_par_passager: ["conducteur_montant_max_par_passager"],
  trajet_longueur_min: ["trajet_longueur_max"],
  passager_eligible_gratuite: ["passager_gratuite"],
  passager_eligibilite_reduction: ["passager_reduction_ticket"],
  zone_exclusion_liste: ["zone_exclusion"],
};

export type FieldErrors = Record<string, string[] | undefined>;

// Première erreur de chaque champ touché (ou de tous au submit).
export const visibleErrors = (
  errors: FieldErrors,
  touched: ReadonlySet<string>,
  submitted: boolean,
): Record<string, string> => {
  const out: Record<string, string> = {};
  for (const [key, messages] of Object.entries(errors)) {
    const message = messages?.[0];
    if (!message) continue;
    if (submitted || touched.has(key) || (linkedFields[key] ?? []).some((k) => touched.has(k))) out[key] = message;
  }
  return out;
};

export type ExternalCampaignForm = z.input<typeof externalCampaignSchema>;

export interface ExternalCampaign extends ExternalCampaignForm {
  id: number;
  status: (typeof campaignStatuses)[number];
  created_by_operator_id: number | null;
  created_by_territory_id: number | null;
}

export const emptyExternalCampaign = (): Omit<ExternalCampaignForm, "territory"> & {
  territory: ExternalCampaignForm["territory"] | null;
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

// Mêmes règles que l'API (externalCampaignAccess.ts) : seuls les admins écrivent ;
// un opérateur modifie ses campagnes mono-opérateur, les multi-opérateurs sont saisies par l'équipe RPC.
export const canCreateCampaign = (role: string) =>
  role === "registry.admin" || role === "operator.admin" || role === "territory.admin";

export const canEditCampaign = (
  campaign: Pick<ExternalCampaign, "operator_ids" | "created_by_operator_id" | "created_by_territory_id">,
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

// La simulation n'existe que côté front (session = registry) : la liste est filtrée par paramètre,
// les actions restent celles de l'équipe RPC.
export const campaignListScope = (
  user: { operator_id: number | null; territory_id: number | null },
  simulatedRole: "operator" | "territory" | undefined,
): { params: { operator_id?: number; territory_id?: number }; pending: boolean } => {
  if (!simulatedRole) return { params: {}, pending: false };
  const id = simulatedRole === "operator" ? user.operator_id : user.territory_id;
  if (!id) return { params: {}, pending: true };
  return { params: simulatedRole === "operator" ? { operator_id: id } : { territory_id: id }, pending: false };
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
export type ExternalCampaignPayload = Omit<ExternalCampaignForm, "territory"> & ApiTerritory;
export type ExternalCampaignApiRow = ExternalCampaignPayload & Omit<ExternalCampaign, keyof ExternalCampaignForm>;

// Champs posés par le serveur : le schéma d'entrée de l'API les refuse (`never`) si une ligne éditée les renvoie.
const SERVER_FIELDS = [
  "created_by_operator_id",
  "created_by_territory_id",
  "status",
  "validated_at",
  "validated_by",
  "created_at",
  "updated_at",
  "deleted_at",
];

export const toApiPayload = ({ territory, ...row }: ExternalCampaignForm): ExternalCampaignPayload => {
  const rest = Object.fromEntries(Object.entries(row).filter(([k]) => !SERVER_FIELDS.includes(k))) as typeof row;
  const payload: ExternalCampaignPayload = {
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
}: ExternalCampaignApiRow): ExternalCampaign => {
  const row: ExternalCampaign = {
    ...rest,
    territory: { type: territory_type, code: territory_code, label: territory_label },
  };
  for (const k of MONTANTS) {
    const v = row[k];
    row[k] = v === null || v === undefined ? null : v / 100;
  }
  return row;
};
