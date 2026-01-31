import { StatelessContextInterface, StatelessRuleHelper, TerritorySelectorsInterface } from "../../interfaces/index.ts";
import { NotEligibleTargetException } from "../exceptions/NotEligibleTargetException.ts";
import { startsAndEndsAt } from "./position.ts";

export const startsAndEndsAtOrThrow: StatelessRuleHelper<TerritorySelectorsInterface> = (
  ctx: StatelessContextInterface,
  selector: TerritorySelectorsInterface,
): boolean => {
  if (!startsAndEndsAt(ctx, selector)) {
    throw new NotEligibleTargetException("Journey start & end not in region");
  }
  return true;
};
