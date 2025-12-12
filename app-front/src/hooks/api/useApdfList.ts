import { ApdfRecord } from "@/interfaces/apdfInterface";
import { useRestQuery } from "./useRestQuery";

interface UseApdfListParams {
  campaign_id: number;
  operator_id?: number | null;
}

/**
 * Hook to fetch APDF (Appel de Fonds) list for a campaign.
 *
 * @example
 * const { data, loading, error, refetch } = useApdfList({
 *   campaign_id: 123,
 *   operator_id: 456
 * });
 */
export function useApdfList(params: UseApdfListParams) {
  return useRestQuery<ApdfRecord[]>(
    "v3",
    "apdf/list",
    params,
    { method: "POST" },
    [params.campaign_id, params.operator_id],
  );
}
