// Environnements où le canal RPC reste ouvert : développement local et suites de tests.
const DEV_ENVS = ["local", "test", "ci"];

/**
 * Le canal `/rpc` est fermé partout ailleurs.
 *
 * Il n'a aucun client : les applications passent par le REST `/v3`, aucune spec publiée ne le
 * documente, et un opérateur ne peut pas l'atteindre (il exige un cookie de session, or
 * `sessionMiddleware` se désactive dès qu'un en-tête `Authorization` est présent). Il exposait
 * en revanche l'ensemble des handlers avec une chaîne de middlewares distincte du REST.
 *
 * `APP_ENABLE_RPC_ENDPOINT=true` force l'ouverture, pour le cas où une exploitation en dépendrait
 * encore ; NODE_ENV et APP_ENV sont vérifiés tous les deux, un désaccord ne doit pas ouvrir la voie.
 */
export function isRpcEndpointEnabled(envs: string | string[], flag: boolean): boolean {
  if (flag === true) return true;

  const list = Array.isArray(envs) ? envs : [envs];
  return list.length > 0 && list.every((env) => DEV_ENVS.includes(env));
}
