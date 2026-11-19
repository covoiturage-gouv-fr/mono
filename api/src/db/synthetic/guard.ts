// Refusés quoi que déclare le profil
const FORBIDDEN_ENVS = ["production", "prod"];

/**
 * Liste blanche portée par le profil : un environnement absent, inconnu ou de production est refusé.
 */
export function assertEnvAllowed(env: string | undefined, allowed: readonly string[]): void {
  const current = env?.trim();
  if (!current) {
    throw new Error("APP_ENV n'est pas défini : génération refusée");
  }
  if (FORBIDDEN_ENVS.includes(current.toLowerCase())) {
    throw new Error(`Génération de données synthétiques interdite en '${current}'`);
  }
  if (!allowed.includes(current)) {
    throw new Error(`APP_ENV='${current}' n'est pas autorisé par le profil (${allowed.join(", ")})`);
  }
}
