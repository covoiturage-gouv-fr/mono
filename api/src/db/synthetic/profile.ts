import { isValidSiret } from "./siret.ts";
import { SyntheticProfile } from "./types.ts";

export function parseReferenceDate(value: string): Date {
  const date = new Date(`${value}T00:00:00Z`);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value) || Number.isNaN(date.getTime())) {
    throw new Error(`Date de référence invalide '${value}', format attendu YYYY-MM-DD`);
  }
  return date;
}

/** Incohérences d'un profil : liste vide si le profil est utilisable */
export function profileErrors(profile: SyntheticProfile): string[] {
  const errors: string[] = [];
  const operators = new Set(profile.operators.map((o) => o.key));
  const territories = new Set(profile.territories.map((t) => t.key));

  for (const { key, siret } of [...profile.operators, ...profile.territories]) {
    if (!isValidSiret(siret)) errors.push(`SIRET invalide pour '${key}' : ${siret}`);
  }
  for (const territory of profile.territories) {
    if (!territory.codes.length) errors.push(`Territoire '${territory.key}' sans code`);
    if (!territory.operators.length) errors.push(`Territoire '${territory.key}' sans opérateur`);
    for (const operator of territory.operators) {
      if (!operators.has(operator)) errors.push(`Opérateur inconnu '${operator}' pour '${territory.key}'`);
    }
  }
  for (const user of profile.users) {
    if (user.operator && user.territory) errors.push(`${user.email} : opérateur et territoire à la fois`);
    if (user.operator && !operators.has(user.operator)) errors.push(`${user.email} : opérateur inconnu`);
    if (user.territory && !territories.has(user.territory)) errors.push(`${user.email} : territoire inconnu`);
    if (user.role !== "registry.admin" && !/^[0-9]{9}$/.test(user.login_siren ?? "")) {
      errors.push(`${user.email} : login_siren absent ou invalide`);
    }
  }
  if (new Set(profile.users.map((u) => u.email)).size !== profile.users.length) {
    errors.push("E-mails en double");
  }
  if (profile.months < 1) errors.push("months doit être supérieur à 0");
  if (profile.carpools.identitiesPerOperator < 2 || profile.carpools.identitiesPerOperator > 1000) {
    errors.push("identitiesPerOperator doit être compris entre 2 et 1000");
  }
  if (profile.operators.length > 10) errors.push("10 opérateurs au plus (tranche de numéros fictifs)");

  return errors;
}
