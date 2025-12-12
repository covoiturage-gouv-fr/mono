import { Periods } from "@/interfaces/vizInterface";
import { useRestQuery } from "./useRestQuery";

interface UseIncentiveGraphParams {
  campaign_id: number;
  period: Periods;
}

interface IncentiveDataPoint {
  month?: number;
  year?: number;
  start_date?: string;
  journeys: number;
  incented_journeys: number;
}

/**
 * Hook to fetch incentive graph data for a campaign.
 */
export function useIncentiveGraph(
  { campaign_id, period }: UseIncentiveGraphParams,
) {
  return useRestQuery<IncentiveDataPoint[]>(
    "v3",
    `dashboard/incentive/${period}/`,
    { campaign_id },
    {},
    [campaign_id, period],
  );
}
