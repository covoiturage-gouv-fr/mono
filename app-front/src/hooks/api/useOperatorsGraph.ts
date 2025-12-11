import { Periods } from "@/interfaces/vizInterface";
import { useRestQuery } from "./useRestQuery";

interface UseOperatorsGraphParams {
  campaign_id: number;
  period: Periods;
}

interface OperatorDataPoint {
  month?: number;
  year?: number;
  start_date?: string;
  operator_name: string;
  journeys: number;
}

/**
 * Hook to fetch operators graph data for a campaign.
 */
export function useOperatorsGraph(
  { campaign_id, period }: UseOperatorsGraphParams,
) {
  return useRestQuery<OperatorDataPoint[]>(
    "v3",
    `dashboard/operators/${period}/`,
    { campaign_id },
    {},
    [campaign_id, period],
  );
}
