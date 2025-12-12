import { TerritoriesInterface } from "@/interfaces/dataInterface";
import { useRestQuery } from "./useRestQuery";

interface UseTerritoriesListParams {
  limit?: number;
  policy?: boolean;
  territory_id?: number | null;
}

/**
 * Hook to fetch territories list.
 *
 * @example
 * const { data, loading, error, refetch } = useTerritoriesList({ limit: 200, policy: true });
 */
export function useTerritoriesList(
  { limit, policy, territory_id }: UseTerritoriesListParams = {},
) {
  return useRestQuery<TerritoriesInterface>(
    "v3",
    "dashboard/territories",
    { limit, ...(policy !== undefined && { policy }) },
    {},
    [limit, policy, territory_id],
  );
}
