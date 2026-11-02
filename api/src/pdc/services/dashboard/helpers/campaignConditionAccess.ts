import { ContextType, ForbiddenException } from "@/ilos/common/index.ts";
import type { ResolvedTerritory } from "@/pdc/services/dashboard/interfaces/CampaignConditionsRepositoryInterface.ts";

export type Caller = {
  registry: boolean;
  operator_id: number | null;
  territory_id: number | null;
  // couple type/code du datalake du territoire appelant, résolu par withTerritory
  territory: Pick<ResolvedTerritory, "territory_type" | "territory_code"> | null;
};
type Owned = {
  territory_type: string;
  territory_code: string;
  operator_ids: number[];
  created_by_operator_id: number | null;
  created_by_territory_id: number | null;
};

// L'équipe RPC se reconnaît à sa permission, pas à l'absence d'operator_id (mode simulation).
// Un périmètre est opérateur XOR territoire : on ne garde que celui de la session.
export function callerOf(
  context: Pick<ContextType, "call">,
  registryPermission = "registry.campaign_condition.update",
): Caller {
  const user = context?.call?.user;
  const permissions = (user?.permissions ?? []) as string[];
  const operator_id = user?.operator_id ?? null;
  return {
    registry: permissions.includes(registryPermission),
    operator_id,
    territory_id: operator_id === null ? user?.territory_id ?? null : null,
    territory: null,
  };
}

export const withTerritory = (caller: Caller, resolved: ResolvedTerritory | null): Caller => ({
  ...caller,
  territory: resolved ? { territory_type: resolved.territory_type, territory_code: resolved.territory_code } : null,
});

const isOnly = (ids: number[], operatorId: number | null) =>
  operatorId !== null && ids.length === 1 && ids[0] === operatorId;

// Un territoire a la main sur les campagnes qu'il a saisies et sur toutes celles qui portent sur lui
// (y compris celles d'un opérateur : il peut corriger ou retirer un brouillon qui bloque sa date).
const territoryOwns = (row: Owned, caller: Caller) =>
  caller.territory_id !== null && (
    row.created_by_territory_id === caller.territory_id ||
    (caller.territory !== null &&
      row.territory_type === caller.territory.territory_type &&
      row.territory_code === caller.territory.territory_code)
  );

// Un opérateur ne déclare que des campagnes dont il est le seul opérateur (multi-opérateurs = équipe RPC
// ou territoire). Un territoire choisit ses opérateurs ; son territoire est imposé par l'API.
export function assertCanCreate(operatorIds: number[], caller: Caller): void {
  if (caller.registry || caller.territory_id !== null) return;
  if (!isOnly(operatorIds, caller.operator_id)) {
    throw new ForbiddenException("Une campagne multi-opérateurs est saisie par l'équipe RPC ou le territoire");
  }
}

export function assertCanWrite(existing: Owned, nextOperatorIds: number[] | null, caller: Caller): void {
  if (caller.registry) return;
  const owns = caller.territory_id !== null
    ? territoryOwns(existing, caller)
    : existing.created_by_operator_id === caller.operator_id && isOnly(existing.operator_ids, caller.operator_id);
  if (!owns) throw new ForbiddenException("Campagne en lecture seule : contactez l'équipe RPC");
  if (nextOperatorIds) assertCanCreate(nextOperatorIds, caller);
}
