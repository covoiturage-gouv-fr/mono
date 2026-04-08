import {
  StatelessContextInterface,
  StatelessRuleHelper,
} from "../../interfaces/index.ts";
import { MisconfigurationException } from "../exceptions/MisconfigurationException.ts";
import { toZonedTime } from "@/pdc/helpers/dates.helper.ts";

interface AtDateParams {
  dates: string[];
  tz?: string;
}

export const atDate: StatelessRuleHelper<AtDateParams> = (
  ctx: StatelessContextInterface,
  params: AtDateParams,
): boolean => {
  const dateStr = toZonedTime(ctx.carpool.datetime, params.tz).toISOString();
  const ctxDate = dateStr.split("T")[0];
  if (!Array.isArray(params.dates)) {
    throw new MisconfigurationException();
  }

  if (params.dates.indexOf(ctxDate) >= 0) {
    return true;
  }
  return false;
};
