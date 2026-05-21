import {
  StatelessContextInterface,
  StatelessRuleHelper,
} from "../../interfaces/index.ts";
import { defaultTimezone } from "@/config/time.ts";
import { toZonedTime } from "@/pdc/helpers/dates.helper.ts";

interface OnWeekdayParams {
  days: number[];
  tz?: string;
}

export const onWeekday: StatelessRuleHelper<OnWeekdayParams> = (
  ctx: StatelessContextInterface,
  params: OnWeekdayParams,
): boolean => {
  // date-fns-tz v3 toZonedTime has no default tz; fall back to Europe/Paris.
  const date = toZonedTime(ctx.carpool.datetime, params.tz ?? defaultTimezone);
  const day = typeof date.getDay === "function"
    ? date.getDay()
    : new Date(date).getDay();
  if (params.days.indexOf(day) < 0) {
    return false;
  }
  return true;
};
