import {
  TerritoryCodeEnum,
  TerritorySelectorsInterface,
} from "../contracts/common/interfaces/TerritoryCodeInterface.ts";
import { TerritoryPerimeterInterface } from "../contracts/common/interfaces/TerritoryPerimeterInterface.ts";

const CODE_FORMATS: Partial<Record<TerritoryCodeEnum, RegExp>> = {
  [TerritoryCodeEnum.Arr]: /^[0-9][0-9AB][0-9]{3}$/,
  [TerritoryCodeEnum.City]: /^[0-9][0-9AB][0-9]{3}$/,
  [TerritoryCodeEnum.CityGroup]: /^[0-9]{9}$/,
  [TerritoryCodeEnum.Mobility]: /^[0-9]{9}$/,
  [TerritoryCodeEnum.District]: /^([0-9]{2,3}|2A|2B)$/,
  [TerritoryCodeEnum.Region]: /^[0-9]{2}$/,
};
const TYPES = Object.keys(CODE_FORMATS);

export type TerritoryOperation = "add" | "remove" | "set";

export function parseTerritoryCodes(tokens: string[]): TerritorySelectorsInterface {
  const selectors: Record<string, Set<string>> = {};
  for (const token of tokens.flatMap((t) => t.split(",")).map((t) => t.trim()).filter(Boolean)) {
    const [rawType, rawCode = "", ...rest] = token.split(":");
    const type = rawType.toLowerCase();
    const code = rawCode.toUpperCase();
    if (rest.length || !TYPES.includes(type)) {
      throw new Error(`Code invalide '${token}', format attendu type:code (${TYPES.join("|")})`);
    }
    if (!CODE_FORMATS[type as TerritoryCodeEnum]!.test(code)) {
      throw new Error(`Code invalide '${token}' pour le type ${type}`);
    }
    (selectors[type] ??= new Set()).add(code);
  }

  if (!Object.keys(selectors).length) {
    throw new Error("Aucun code territoire fourni");
  }

  return Object.fromEntries(Object.entries(selectors).map(([t, c]) => [t, [...c]]));
}

/**
 * Codes that replace the given ones after merges, splits or code changes,
 * following chains across successive millesimes.
 */
export function successors(arr: string[], evolutions: { old_com: string; new_com: string }[]): string[] {
  const known = new Set(arr);
  const found = new Set<string>();
  let frontier = [...known];
  while (frontier.length) {
    const next = evolutions
      .filter((e) => frontier.includes(e.old_com) && !known.has(e.new_com))
      .map((e) => e.new_com);
    next.forEach((c) => {
      known.add(c);
      found.add(c);
    });
    frontier = next;
  }
  return [...found].sort();
}

function covers(v: TerritoryPerimeterInterface, d: Date): boolean {
  return v.valid_from <= d && (v.valid_to === null || d < v.valid_to);
}

export function findVersionAt(
  versions: TerritoryPerimeterInterface[],
  d: Date,
): TerritoryPerimeterInterface | undefined {
  return versions
    .filter((v) => covers(v, d))
    .reduce<TerritoryPerimeterInterface | undefined>(
      (best, v) => (!best || v.version > best.version ? v : best),
      undefined,
    );
}

export function applyOperation(op: TerritoryOperation, current: string[], resolved: string[]): string[] {
  const result = new Set(op === "set" ? [] : current);
  for (const arr of resolved) {
    op === "remove" ? result.delete(arr) : result.add(arr);
  }
  return [...result].sort();
}

export function diffArr(before: string[], after: string[]): { added: string[]; removed: string[] } {
  const b = new Set(before);
  const a = new Set(after);
  return {
    added: after.filter((x) => !b.has(x)).sort(),
    removed: before.filter((x) => !a.has(x)).sort(),
  };
}
