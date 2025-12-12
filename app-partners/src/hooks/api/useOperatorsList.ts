import { OperatorsInterface } from "@/interfaces/dataInterface";
import { useRestQuery } from "./useRestQuery";

interface UseOperatorsListParams {
  limit?: number;
  page?: number;
  search?: string;
  pagination?: boolean;
  id?: number | null;
}

/**
 * Hook to fetch operators list.
 *
 * @example
 * const { data, loading, error, refetch } = useOperatorsList({ limit: 100 });
 */
export function useOperatorsList(
  { limit, search, page, pagination = true, id }: UseOperatorsListParams = {},
) {
  return useRestQuery<OperatorsInterface>(
    "v3",
    "dashboard/operators",
    {
      limit,
      ...(search && { search }),
      ...(page && { page }),
      ...(id && { id }),
    },
    { paginate: pagination },
    [limit, search, page, id],
  );
}
