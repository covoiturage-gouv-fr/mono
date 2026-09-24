import { PolicyTerritoryInterface, TerritoryCode, TerritoryCodeEnum } from "../interfaces/index.ts";

const CODE_FORMATS: Record<TerritoryCodeEnum, RegExp> = {
  [TerritoryCodeEnum.Arr]: /^[0-9][0-9AB][0-9]{3}$/,
  [TerritoryCodeEnum.City]: /^[0-9][0-9AB][0-9]{3}$/,
  [TerritoryCodeEnum.CityGroup]: /^[0-9]{9}$/,
  [TerritoryCodeEnum.Mobility]: /^[0-9]{9}$/,
  [TerritoryCodeEnum.District]: /^([0-9]{2,3}|2A|2B)$/,
  [TerritoryCodeEnum.Region]: /^[0-9]{2}$/,
  [TerritoryCodeEnum.Network]: /^[0-9]+$/,
  [TerritoryCodeEnum.Country]: /^[0-9X]{5}$/,
};
const TYPES = Object.values(TerritoryCodeEnum) as string[];

export type TerritoryOperation = "add" | "remove" | "set";

export function parseTerritoryCodes(tokens: string[]): TerritoryCode[] {
  const codes = new Map<string, TerritoryCode>();
  for (const token of tokens.flatMap((t) => t.split(",")).map((t) => t.trim()).filter(Boolean)) {
    const [type, code, ...rest] = token.split(":");
    if (rest.length || !TYPES.includes(type)) {
      throw new Error(`Code invalide '${token}', format attendu type:code (${TYPES.join("|")})`);
    }
    if (!CODE_FORMATS[type as TerritoryCodeEnum].test(code)) {
      throw new Error(`Code invalide '${token}' pour le type ${type}`);
    }
    codes.set(token, { type: type as TerritoryCodeEnum, code });
  }

  if (!codes.size) {
    throw new Error("Aucun code territoire fourni");
  }

  return [...codes.values()];
}

function covers(v: PolicyTerritoryInterface, d: Date): boolean {
  return v.valid_from <= d && (v.valid_to === null || d < v.valid_to);
}

export function findVersionAt(
  versions: PolicyTerritoryInterface[],
  d: Date,
): PolicyTerritoryInterface | undefined {
  return versions
    .filter((v) => covers(v, d))
    .reduce<PolicyTerritoryInterface | undefined>(
      (best, v) => (!best || v.version > best.version ? v : best),
      undefined,
    );
}

export function overlapping(
  versions: PolicyTerritoryInterface[],
  from: Date,
  to: Date,
): PolicyTerritoryInterface[] {
  return versions.filter((v) => v.valid_from < to && (v.valid_to === null || v.valid_to > from));
}

export function isCovered(versions: PolicyTerritoryInterface[], from: Date, to: Date): boolean {
  let cursor = from.getTime();
  for (const v of [...versions].sort((a, b) => a.valid_from.getTime() - b.valid_from.getTime())) {
    if (v.valid_from.getTime() > cursor) break;
    cursor = Math.max(cursor, v.valid_to?.getTime() ?? Infinity);
    if (cursor >= to.getTime()) return true;
  }
  return cursor >= to.getTime();
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
