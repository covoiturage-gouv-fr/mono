import { TerritoryCodeInterface, TerritorySelectorsInterface } from "../../interfaces/index.ts";

export function isSelected(
  code: TerritoryCodeInterface,
  selectors: TerritorySelectorsInterface,
): boolean {
  if (!code) return false;
  for (const key of Object.keys(selectors)) {
    const value = code[key];
    if (selectors[key].indexOf(value) >= 0) {
      return true;
    }
  }
  return false;
}
