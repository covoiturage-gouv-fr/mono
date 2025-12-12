import { env_or_fail } from "@/lib/env/index.ts";

export const ADMIN_EMAIL = env_or_fail("APIE2E_AUTH_ADMIN_EMAIL", "admin@example.com");
export const ADMIN_PASSWORD = env_or_fail("APIE2E_AUTH_ADMIN_PASSWORD", "admin1234");
export const OPERATOR_EMAIL = env_or_fail("APIE2E_AUTH_OPERATOR_EMAIL", "operator@example.com");
export const OPERATOR_PASSWORD = env_or_fail("APIE2E_AUTH_OPERATOR_PASSWORD", "admin1234");
export const TERRITORY_EMAIL = env_or_fail("APIE2E_AUTH_TERRITORY_EMAIL", "territory@example.com");
export const TERRITORY_PASSWORD = env_or_fail("APIE2E_AUTH_TERRITORY_PASSWORD", "admin1234");

export const accounts = new Map<string, string>([
  [ADMIN_EMAIL, ADMIN_PASSWORD],
  [OPERATOR_EMAIL, OPERATOR_PASSWORD],
  [TERRITORY_EMAIL, TERRITORY_PASSWORD],
]);
