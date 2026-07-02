import { StatelessContextInterface, StatelessRuleHelper } from "../../interfaces/index.ts";
import { defaultTimezone } from "@/config/time.ts";
import { toTzString } from "@/pdc/helpers/dates.helper.ts";

interface OnHourRangeParams {
  // hours of the day as decimals in the [0, 24] range
  start: number;
  end: number;
  tz?: string;
}

/**
 * Returns true when the carpool departure time-of-day falls within
 * [start, end) in the given timezone (Europe/Paris by default).
 * Minutes are taken into account (e.g. 03:30 => 3.5).
 */
export const onHourRange: StatelessRuleHelper<OnHourRangeParams> = (
  ctx: StatelessContextInterface,
  params: OnHourRangeParams,
): boolean => {
  // Extract the wall-clock time in the target timezone, independent of the
  // runtime timezone (formatInTimeZone under the hood).
  const [hh, mm] = toTzString(ctx.carpool.datetime, params.tz ?? defaultTimezone, "HH:mm")
    .split(":")
    .map(Number);
  const hours = hh + mm / 60;
  return hours >= params.start && hours < params.end;
};
