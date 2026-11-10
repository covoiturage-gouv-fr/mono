import { type Violation, parseViolations, violationReason } from "@/helpers/apiViolations";
import { apiErrorMessage } from "@/hooks/useApi";

// Libellés du formulaire (ExternalCampaignForm.tsx) par champ de l'API.
const FIELD_LABELS: Record<string, string> = {
  territory_type: "Territoire",
  territory_code: "Territoire",
  territory_label: "Territoire",
  operator_ids: "Opérateurs participants",
  premiere_campagne: "Première campagne sur ce territoire",
  date_debut: "Date de début",
  date_fin: "Date de fin",
  budget_incitations: "Budget des incitations",
  conducteur_montant_min_par_passager: "Montant minimum par passager",
  conducteur_montant_max_par_passager: "Montant maximum par passager",
  conducteur_montant_max_par_mois: "Plafond mensuel",
  conducteur_trajets_max_par_mois: "Trajets maximum par mois (conducteur)",
  passager_gratuite: "Trajet gratuit pour le passager",
  passager_eligible_gratuite: "Qui bénéficie de la gratuité ?",
  passager_reduction_ticket: "Réduction sur le prix du trajet",
  passager_eligibilite_reduction: "Qui bénéficie de la réduction ?",
  passager_montant_ticket: "Montant de la réduction",
  passager_trajets_max_par_mois: "Trajets maximum par mois (passager)",
  zone_sens_des_trajets: "Sens des trajets",
  zone_exclusion: "Des zones sont exclues",
  zone_exclusion_liste: "Zones exclues",
  autre_exclusion: "Autres exclusions",
  trajet_longueur_min: "Longueur minimale",
  trajet_longueur_max: "Longueur maximale",
  trajet_classe_preuve: "Classes de preuve acceptées",
  lien_page_collectivite: "Lien vers la page de la collectivité",
  autres_informations: "Autres informations",
};

// Règles croisées de crossRuleError (api/src/pdc/services/dashboard/dto/ExternalCampaigns.ts), sans chemin.
const CROSS_RULES: Record<string, string> = {
  "date_fin doit suivre date_debut": "La date de fin doit être postérieure à la date de début.",
  "montant min par passager supérieur au max": "Le montant minimum par passager dépasse le montant maximum.",
  "longueur min supérieure à la max": "La longueur minimale dépasse la longueur maximale.",
  "éligibilité à la gratuité requise": "Indiquez qui bénéficie de la gratuité.",
  "éligibilité à la réduction requise": "Indiquez qui bénéficie de la réduction.",
  "liste des zones exclues requise": "Renseignez les zones exclues.",
};

const MAX_LINES = 10;
const MAX_LINE_LENGTH = 300;

const violationLines = (violations: Violation[]): string[] => {
  const lines = new Map<string, string>();
  for (const { path, message } of violations) {
    const [field] = path;
    const label = field ? (FIELD_LABELS[field] ?? field) : undefined;
    const line = label ? `${label} : ${violationReason(message)}` : (CROSS_RULES[message] ?? violationReason(message));
    // Un seul message par champ : le premier, le plus général.
    const key = label ?? line;
    if (!lines.has(key)) lines.set(key, line.slice(0, MAX_LINE_LENGTH));
  }
  return [...lines.values()].slice(0, MAX_LINES);
};

// Une ligne par problème, séparées par « \n » ; le repli reste apiErrorMessage.
export const campaignErrorMessage = (status: number, body: unknown): string => {
  // Les 403 des campagnes portent un motif métier en français, contrairement aux autres pages.
  if (status === 403 && typeof body === "string" && body.trim()) return body.slice(0, MAX_LINE_LENGTH);
  const violations = status === 400 ? parseViolations(body) : undefined;
  return violations ? violationLines(violations).join("\n") : apiErrorMessage(status, body);
};
