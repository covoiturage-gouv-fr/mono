import { env_or_default, env_or_int } from "@/lib/env/index.ts";

/**
 * -----------------------------------------------------------------------------
 * EXPORT CONFIGURATION
 * -----------------------------------------------------------------------------
 */

/**
 * Oldest date an export can be created from.
 * @default 3 years ago
 */
export const minStartDefault = env_or_int(
  "APP_EXPORT_MIN_START",
  1000 * 60 * 60 * 24 * 365 * 3,
);

/**
 * Latest date an export can be created from.
 * @default 5 days ago (needed to finish processing)
 */
export const maxEndDefault = env_or_int(
  "APP_EXPORT_MAX_END",
  1000 * 60 * 60 * 24 * 5,
);

/**
 * How much time before an export is considered stale.
 * The syntax is using PostgreSQL interval format.
 * @see https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-INTERVAL-INPUT
 */
export const staleDelay = env_or_default("APP_EXPORT_STALE_DELAY", "4 hours");
