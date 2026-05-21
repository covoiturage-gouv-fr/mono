import {
  StatelessContextInterface,
  StatelessRuleHelper,
} from "../../interfaces/index.ts";
import { MisconfigurationException } from "../exceptions/MisconfigurationException.ts";
import { defaultTimezone } from "@/config/time.ts";
import { toZonedTime } from "@/pdc/helpers/dates.helper.ts";

interface AtDateParams {
  dates: string[];
  tz?: string;
}

export const atDate: StatelessRuleHelper<AtDateParams> = (
  ctx: StatelessContextInterface,
  params: AtDateParams,
): boolean => {
  // date-fns-tz v3 toZonedTime has no default tz; an undefined tz shifts the
  // instant by the host UTC offset, so a UTC-midnight carpool date can roll
  // back to the previous day. Fall back to Europe/Paris to match policy tz.
  const dateStr = toZonedTime(ctx.carpool.datetime, params.tz ?? defaultTimezone).toISOString();
  const ctxDate = dateStr.split("T")[0];
  if (!Array.isArray(params.dates)) {
    throw new MisconfigurationException();
  }

  if (params.dates.indexOf(ctxDate) >= 0) {
    return true;
  }
  return false;
};
