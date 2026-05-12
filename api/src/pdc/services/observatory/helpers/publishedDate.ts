import { config } from "../config/index.ts";

type PeriodParams = {
  year: number;
  month?: number;
  trimester?: number;
  semester?: number;
};

/**
 * Compute the first day of a requested period.
 * E.g. month=2 -> Feb 1st; trimester=1 -> Jan 1st; year=2026 -> Jan 1st.
 */
export function getPeriodStart(params: PeriodParams): Date {
  if (params.month !== undefined) {
    return new Date(params.year, params.month - 1, 1);
  }
  if (params.trimester !== undefined) {
    return new Date(params.year, (params.trimester - 1) * 3, 1);
  }
  if (params.semester !== undefined) {
    return new Date(params.year, (params.semester - 1) * 6, 1);
  }
  return new Date(params.year, 0, 1);
}

/**
 * Check if a requested period is published based on
 * APP_OBSERVATORY_PUBLISHED_UNTIL (exclusive upper bound).
 *
 * Uses rolling logic: a period is visible if its start date
 * is strictly before the cutoff. This means partial trimesters,
 * semesters, and years are shown as long as they contain data
 * before the cutoff.
 *
 * Returns true (published) when:
 * - config value is unset (backwards compatible, no gate)
 * - the period start date is strictly before the cutoff date
 *
 * NOTE: This assumes no response caching on observatory routes.
 * If route caching is added, cutoff changes require cache invalidation.
 */
export function isPublished(params: PeriodParams): boolean {
  const cutoff = config.observatory.publishedUntil;
  if (!cutoff) return true;

  // Parse YYYY-MM-DD as local time to match getPeriodStart (also local time).
  // new Date("2026-03-01") would create UTC midnight, causing timezone mismatches.
  const parts = cutoff.split("-").map(Number);
  if (parts.length !== 3 || parts.some(isNaN)) return false;
  const cutoffDate = new Date(parts[0], parts[1] - 1, parts[2]);

  const periodStart = getPeriodStart(params);
  return periodStart < cutoffDate;
}
