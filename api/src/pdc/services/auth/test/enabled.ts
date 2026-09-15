// Liste blanche : la route de login de test ouvre une session sur un compte réel de la base.
// Un environnement inconnu (staging, preview, faute de frappe) doit donc la refuser par défaut.
const ALLOWED_ENVS = ["local", "test", "ci"];

// Both NODE_ENV and APP_ENV are checked: a mismatch between them must not open the route
export function isTestAuthEnabled(envs: string | string[], flag: boolean): boolean {
  const list = Array.isArray(envs) ? envs : [envs];
  return flag === true && list.length > 0 && list.every((env) => ALLOWED_ENVS.includes(env));
}
