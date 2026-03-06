import { env } from "./lib/config.ts";

/**
 * Test for legacy authentication with different API versions.
 * @doc https://tech.covoiturage.beta.gouv.fr/
 *
 * UPDATE the list of supported versions if needed.
 * - 2025-05-22: v3, v3.2
 * - 2025-07-04: v3, v3.2, v3.3
 * - 2026-03-03: v3, v3.3
 * - 2026-03-04: v3, v3.3, v3.4
 */
export const SUPPORTED_VERSIONS = ["v3", "v3.3", "v3.4"];
export const UNSUPPORTED_VERSIONS = ["v3.1", "v3.2", "v4"];

export const ADMIN_EMAIL = env("APIE2E_AUTH_ADMIN_EMAIL", "admin@example.com");
export const ADMIN_PASSWORD = env("APIE2E_AUTH_ADMIN_PASSWORD", "admin1234");
export const OPERATOR_EMAIL = env("APIE2E_AUTH_OPERATOR_EMAIL", "operator@example.com");
export const OPERATOR_PASSWORD = env("APIE2E_AUTH_OPERATOR_PASSWORD", "admin1234");
export const TERRITORY_EMAIL = env("APIE2E_AUTH_TERRITORY_EMAIL", "territory@example.com");
export const TERRITORY_PASSWORD = env("APIE2E_AUTH_TERRITORY_PASSWORD", "admin1234");
