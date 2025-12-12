import { OperatorsInterface } from "@/interfaces/dataInterface";
import { useRestQuery } from "./useRestQuery";

interface UseOperatorsListParams {
  policy_id?: number;
  descriptive_sheet_url?: string;
}

/**
 * Hook to fetch operators list.
 *
 * @example
 * const { data, loading, error, refetch } = usePolicyDescriptiveSheetUpdate({ policy_id: 1, descriptive_sheet_url: "https://example.com" });
 */
export function usePolicyDescriptiveSheetUpdate(
  { policy_id, descriptive_sheet_url }: UseOperatorsListParams = {},
) {
  return useRestQuery<OperatorsInterface>(
    "v3",
    "dashboard/policies/:policy_id/descriptive-sheet-url",
    {
      policy_id,
      ...(descriptive_sheet_url && { descriptive_sheet_url }),
    },
    { method: "POST" },
    [policy_id, descriptive_sheet_url],
  );
}
