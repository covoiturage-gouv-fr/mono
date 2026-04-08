import {
  StatelessContextInterface,
  StatelessRuleHelper,
} from "../../interfaces/index.ts";
import { toZonedTime } from "@/pdc/helpers/dates.helper.ts";

interface OnWeekdayParams {
  days: number[];
  tz?: string;
}

export const onWeekday: StatelessRuleHelper<OnWeekdayParams> = (
  ctx: StatelessContextInterface,
  params: OnWeekdayParams,
): boolean => {
  const date = toZonedTime(ctx.carpool.datetime, params.tz);
  const day = typeof date.getDay === "function"
    ? date.getDay()
    : new Date(date).getDay();
  if (params.days.indexOf(day) < 0) {
    return false;
  }
  return true;
};
