import { config } from "../config/index.ts";

type PeriodParams = {
  year: number;
  month?: number;
  trimester?: number;
  semester?: number;
};

/**
 * Compute the exclusive upper bound date for a requested period.
 * E.g. month=2 (Feb) -> March 1st; year=2026 -> Jan 1st 2027.
 */
export function getPeriodEnd(params: PeriodParams): Date {
  if (params.month !== undefined) {
    // month is 1-based in params, 0-based in Date() => params.month maps to next month
    return new Date(params.year, params.month, 1);
  }
  if (params.trimester !== undefined) {
    return new Date(params.year, params.trimester * 3, 1);
  }
  if (params.semester !== undefined) {
    return new Date(params.year, params.semester * 6, 1);
  }
  return new Date(params.year + 1, 0, 1);
}

/**
 * Check if a requested period is published based on
 * APP_OBSERVATORY_PUBLISHED_UNTIL (exclusive upper bound).
 *
 * Returns true (published) when:
 * - config value is unset (backwards compatible, no gate)
 * - the period end date is <= the cutoff date
 *
 * NOTE: This assumes no response caching on observatory routes.
 * If route caching is added, cutoff changes require cache invalidation.
 */
export function isPublished(params: PeriodParams): boolean {
  const cutoff = config.observatory.publishedUntil;
  if (!cutoff) return true;

  const cutoffDate = new Date(cutoff);
  if (isNaN(cutoffDate.getTime())) return false;

  const periodEnd = getPeriodEnd(params);
  return periodEnd <= cutoffDate;
}
