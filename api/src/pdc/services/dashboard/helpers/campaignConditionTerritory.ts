import { InvalidRequestException } from "@/ilos/common/index.ts";
import { Caller, withTerritory } from "@/pdc/services/dashboard/helpers/campaignConditionAccess.ts";
import type { ResolvedTerritory } from "@/pdc/services/dashboard/interfaces/CampaignConditionsRepositoryInterface.ts";

export type TerritoryGroupShape = {
  name: string;
  has_versions: boolean;
  selectors: { type: string; value: string }[];
};

const GEO_TYPES = new Set(["com", "epci", "aom", "dep", "reg", "country"]);

// Couple (type, code) du datalake pour un territory_group : le datalake ne calcule en `custom` que les
// territoires à périmètre versionné (code = territory_id) ; sinon seul un sélecteur unique a un équivalent.
export function toCampaignTerritory(territoryId: number, group: TerritoryGroupShape): ResolvedTerritory | null {
  if (group.has_versions) {
    return { territory_type: "custom", territory_code: String(territoryId), territory_label: group.name };
  }
  const [only, ...others] = group.selectors;
  if (!only || others.length || !GEO_TYPES.has(only.type)) return null;
  return {
    territory_type: only.type as ResolvedTerritory["territory_type"],
    territory_code: only.value,
    territory_label: group.name,
  };
}

export async function resolveOwnTerritory(
  repository: { resolveTerritory(id: number): Promise<ResolvedTerritory | null> },
  territoryId: number,
): Promise<ResolvedTerritory> {
  const resolved = await repository.resolveTerritory(territoryId);
  if (!resolved) {
    throw new InvalidRequestException(
      "Votre territoire n'a pas d'équivalent dans le datalake : contactez l'équipe RPC pour déclarer la campagne",
    );
  }
  return resolved;
}

// Complète un appelant territoire avec son couple type/code du datalake (null si sans équivalent).
export async function withCallerTerritory(
  repository: { resolveTerritory(id: number): Promise<ResolvedTerritory | null> },
  caller: Caller,
): Promise<Caller> {
  if (caller.registry || caller.territory_id === null) return caller;
  return withTerritory(caller, await repository.resolveTerritory(caller.territory_id));
}
