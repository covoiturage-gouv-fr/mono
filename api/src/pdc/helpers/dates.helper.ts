import { defaultTimezone } from "@/config/time.ts";
import { addDays, addMonths, endOfDay as eod, startOfDay as sod, startOfMonth, subDays, subMonths } from "dep:date-fns";
import { formatInTimeZone, fromZonedTime, toZonedTime } from "dep:date-fns-tz";

/**
 * Regular UTC toISOString.
 *
 * The same as native toISOString but safer.
 *
 * @example toISOString(new Date())
 *
 * @param Date
 * @returns string
 */
export function toISOString(d: Date): string {
  try {
    return d.toISOString();
  } catch {
    return d?.toString();
  }
}

/**
 * Convert a Date to a string in the User timezone
 *
 * Default string format is yyyy-MM-dd'T'HH:mm:ssXX
 *
 * @example toTzString(new Date(), 'Europe/Paris', 'yyyy-MM-dd')
 *
 * @param Date | string | number
 * @param Timezone
 * @param string
 * @returns string
 */
export function toTzString(
  d: Date | string | number,
  tz = defaultTimezone,
  format = "yyyy-MM-dd'T'HH:mm:ssXX",
): string {
  try {
    return formatInTimeZone(d, tz || defaultTimezone, format);
  } catch {
    return d?.toString();
  }
}

export function castUserStringToUTC(
  d: Date | string | undefined | null,
  tz = defaultTimezone,
): Date | undefined {
  if (d === null || typeof d === "undefined") return;

  // a JS Date doesn't need to be offset.
  // the User must set the right timezone by himself
  if (typeof d !== "string") {
    return d;
  }

  // a short-form date (YYYY-MM-DD) has no timezone
  // the User timezone is applied and the date is converted to UTC
  if (/^[0-9]{4}-[0-9]{2}-[0-9]{2}$/.test(d)) {
    return fromZonedTime(d, tz);
  }

  // a full-short date (YYYY-MM-DDTHH:mm:ssZZ) has a timezone
  // it is converted to UTC by the Date object
  const dd = new Date(d);
  if (dd.toString() === "Invalid Date") return;

  return dd;
}

export function dateWithTz(date: Date, tz = defaultTimezone): Date {
  return fromZonedTime(
    date.toISOString().substring(0, 11),
    tz || defaultTimezone,
  );
}

/**
 * Local today
 *
 * @example today()
 * @example today('Europe/Paris')
 */
export function today(tz = defaultTimezone): Date {
  return dateWithTz(new Date(), tz);
}

export function addDaysTz(d: Date, days: number, tz = defaultTimezone): Date {
  return fromZonedTime(
    addDays(toZonedTime(d, tz || defaultTimezone), days),
    tz || defaultTimezone,
  );
}

export function subDaysTz(d: Date, days: number, tz = defaultTimezone): Date {
  return fromZonedTime(
    subDays(toZonedTime(d, tz || defaultTimezone), days),
    tz || defaultTimezone,
  );
}

export function addMonthsTz(
  d: Date,
  months: number,
  tz = defaultTimezone,
): Date {
  return fromZonedTime(
    addMonths(toZonedTime(d, tz || defaultTimezone), months),
    tz || defaultTimezone,
  );
}

export function subMonthsTz(
  d: Date,
  months: number,
  tz = defaultTimezone,
): Date {
  return fromZonedTime(
    subMonths(toZonedTime(d, tz || defaultTimezone), months),
    tz || defaultTimezone,
  );
}

export function startOfMonthTz(d: Date, tz = defaultTimezone): Date {
  return fromZonedTime(
    startOfMonth(toZonedTime(d, tz || defaultTimezone)),
    tz || defaultTimezone,
  );
}

export function startOfDay(d: Date, tz = defaultTimezone): Date {
  return fromZonedTime(
    sod(toZonedTime(d, tz)),
    tz,
  );
}

export function endOfDay(d: Date, tz = defaultTimezone): Date {
  return fromZonedTime(
    eod(toZonedTime(d, tz)),
    tz,
  );
}

/**
 * Fractional difference in hours between two dates.
 *
 * @example differenceInHours(new Date('2024-10-24T06:37:09'), new Date('2024-10-23T05:00:47'))
 * // => 25.606...
 */
export function differenceInHours(d1: Date, d2: Date): number {
  return (d1.getTime() - d2.getTime()) / 3_600_000;
}

/**
 * Add (or subtract with negative values) minutes to a date.
 */
export function addMinutes(d: Date, minutes: number): Date {
  return new Date(d.getTime() + minutes * 60_000);
}

// Re-export for policy engine consumers (previously in a deprecated wrapper)
export { toZonedTime };
