import { CampaignsInterface } from "@/interfaces/dataInterface";
import { useRestQuery } from "./useRestQuery";

interface UseCampaignListParams {
  territory_id?: number | null;
  operator_id?: number | null;
  search?: string;
}

/**
 * Hook to fetch campaign list with optional filters.
 *
 * @example
 * const { data, loading, error, refetch } = useCampaignList({
 *   territory_id: 123,
 *   search: "Paris"
 * });
 */
export function useCampaignList(
  params: UseCampaignListParams = {},
) {
  return useRestQuery<CampaignsInterface[]>("v3", "campaign/list", params, {
    method: "POST",
  }, [params.territory_id, params.operator_id, params.search]);
}
