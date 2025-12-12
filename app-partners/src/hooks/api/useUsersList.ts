import { UsersInterface } from "@/interfaces/dataInterface";
import { useRestQuery } from "./useRestQuery";

interface UseUsersListParams {
  territoryId?: number | null;
  operatorId?: number | null;
  page?: number;
  search?: string;
}

/**
 * Hook to fetch users list with optional filters.
 */
export function useUsersList(
  { territoryId, operatorId, page, search }: UseUsersListParams = {},
) {
  return useRestQuery<UsersInterface>(
    "v3",
    "dashboard/users",
    {
      ...(territoryId && { territory_id: territoryId }),
      ...(operatorId && { operator_id: operatorId }),
      ...(page && page !== 1 && { page }),
      ...(search && { search }),
    },
    {},
    [territoryId, operatorId, page, search],
  );
}
