import { isRegistry } from "@/helpers/auth";
import { useAuth } from "@/providers/AuthProvider";

// Opérateur / territoire sur lesquels le compte (ou l'équipe RPC qui simule) déclare ses campagnes.
export function useCampaignContext() {
  const { user, activeScope, simulatedRole, simulatedLabel } = useAuth();
  const registry = isRegistry(user?.role);
  const operatorId = user?.operator_id ?? null;
  const simulatingTerritory = registry && simulatedRole === "territory";
  const territoryId = simulatingTerritory
    ? (user?.territory_id ?? null)
    : !registry && operatorId === null
      ? (user?.territory_id ?? null)
      : null;
  const territoryLabel =
    territoryId === null
      ? null
      : simulatingTerritory
        ? (simulatedLabel ?? "Territoire simulé")
        : (activeScope?.label ?? user?.organisation ?? "Votre territoire");
  return { user, simulatedRole, registry, operatorId, simulatingTerritory, territoryId, territoryLabel };
}
