import { Campaign } from "@/interfaces/campaignInterface";
import { useRestQuery } from "./useRestQuery";

interface UseCampaignFindParams {
  id: number;
}

/**
 * Hook to fetch a single campaign by ID.
 *
 * @example
 * const { data, loading, error, refetch } = useCampaignFind({ id: 123 });
 */
export function useCampaignFind({ id }: UseCampaignFindParams) {
  return useRestQuery<Campaign>("v3", "campaign/find", { _id: id }, {
    method: "POST",
  }, [id]);
}
