import { env_or_default, env_or_fail, env_or_false } from "@/lib/env/index.ts";

export const enabled = env_or_false("APP_ENABLE_TEST_AUTH");

// Lazy: env_or_fail must not run at boot in environments where test auth is off
export function accounts(): Map<string, string> {
  const map = new Map<string, string>([
    [env_or_fail("APIE2E_AUTH_ADMIN_EMAIL"), env_or_fail("APIE2E_AUTH_ADMIN_PASSWORD")],
    [env_or_fail("APIE2E_AUTH_OPERATOR_EMAIL"), env_or_fail("APIE2E_AUTH_OPERATOR_PASSWORD")],
    [env_or_fail("APIE2E_AUTH_TERRITORY_EMAIL"), env_or_fail("APIE2E_AUTH_TERRITORY_PASSWORD")],
  ]);

  // Compte multi-périmètre : optionnel, les stacks qui ne le configurent pas restent valides.
  const multiEmail = env_or_default("APIE2E_AUTH_MULTI_EMAIL", "");
  const multiPassword = env_or_default("APIE2E_AUTH_MULTI_PASSWORD", "");
  if (multiEmail && multiPassword) map.set(multiEmail, multiPassword);

  return map;
}
