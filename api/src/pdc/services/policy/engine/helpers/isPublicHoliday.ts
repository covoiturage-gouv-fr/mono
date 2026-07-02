import { StatelessContextInterface } from "../../interfaces/index.ts";
import { defaultTimezone } from "@/config/time.ts";
import { toTzString } from "@/pdc/helpers/dates.helper.ts";

interface IsPublicHolidayParams {
  tz?: string;
}

// French public holidays (jours fériés légaux) for the ECOV x IDFM campaign
// window. Includes the Easter-based days (lundi de Pâques, Ascension, lundi de
// Pentecôte). Lundi de Pentecôte stays listed here as it is an official
// holiday; the "journée de solidarité" carve-out is a campaign-level decision
// handled in the policy, not here.
export const frenchPublicHolidays: readonly string[] = [
  // 2025 — Pâques 20/04
  "2025-01-01", // Jour de l'an
  "2025-04-21", // Lundi de Pâques
  "2025-05-01", // Fête du travail
  "2025-05-08", // Victoire 1945
  "2025-05-29", // Ascension
  "2025-06-09", // Lundi de Pentecôte
  "2025-07-14", // Fête nationale
  "2025-08-15", // Assomption
  "2025-11-01", // Toussaint
  "2025-11-11", // Armistice 1918
  "2025-12-25", // Noël
  // 2026 — Pâques 05/04
  "2026-01-01",
  "2026-04-06",
  "2026-05-01",
  "2026-05-08",
  "2026-05-14", // Ascension
  "2026-05-25", // Lundi de Pentecôte
  "2026-07-14",
  "2026-08-15",
  "2026-11-01",
  "2026-11-11",
  "2026-12-25",
  // 2027 — Pâques 28/03
  "2027-01-01",
  "2027-03-29",
  "2027-05-01",
  "2027-05-06", // Ascension
  "2027-05-08",
  "2027-05-17", // Lundi de Pentecôte
  "2027-07-14",
  "2027-08-15",
  "2027-11-01",
  "2027-11-11",
  "2027-12-25",
];

/**
 * Returns true when the carpool departure day is a French public holiday,
 * resolved in the given timezone (Europe/Paris by default).
 */
export function isPublicHoliday(
  ctx: StatelessContextInterface,
  params: IsPublicHolidayParams = {},
): boolean {
  const day = toTzString(ctx.carpool.datetime, params.tz ?? defaultTimezone, "yyyy-MM-dd");
  return frenchPublicHolidays.includes(day);
}
