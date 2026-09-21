const DEFAULT_APP_URL = "http://localhost:4200";

// Une seule URL de front. `APP_DASHBOARD_V2_URL` est l'ancien nom, encore posé par les manifestes.
export function pickAppUrl(appUrl?: string, legacy?: string, fallback = DEFAULT_APP_URL): string {
  return appUrl || legacy || fallback;
}

// Deux noms posés avec des valeurs différentes : l'un des deux est ignoré, autant le dire au démarrage.
export function appUrlsDiverge(appUrl?: string, legacy?: string): boolean {
  return !!appUrl && !!legacy && appUrl !== legacy;
}

/**
 * Rappel OIDC déduit de l'origine de l'API.
 *
 * Saisir cette origine une seconde fois dans une variable dédiée finit toujours par diverger de
 * `APP_API_URL`, dont `AuthRouter` reconstruit l'URL de rappel : le fournisseur reçoit alors à
 * l'autorisation et à l'échange du code deux `redirect_uri` différents, et rejette en `invalid_grant`.
 */
export function callbackUrl(apiUrl: string, path: string): string {
  return new URL(path, apiUrl).toString();
}

// Origines CORS : plusieurs variables, une seule liste, séparateur virgule.
export function mergeOrigins(...values: (string | undefined)[]): string[] {
  const origins = values
    .filter((v): v is string => !!v)
    .flatMap((v) => v.split(","))
    .map((v) => v.trim())
    .filter((v) => v.length > 0);

  return [...new Set(origins)];
}
