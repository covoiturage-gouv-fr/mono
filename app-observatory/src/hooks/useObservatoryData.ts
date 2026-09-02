import { useDashboardContext } from "@/context/DashboardProvider";
import { GetApiUrl } from "@/helpers/api";
import { useApi } from "@/hooks/useApi";

// Récupère un endpoint de l'API observatoire pour le territoire et la période
// courants du dashboard. `code` / `type` / `year` sont ajoutés d'office ; passer
// le reste (`indic=…`, `direction=both`, …) dans `extra`. `GetApiUrl` ajoute en
// plus les paramètres de période (month / trimester / semester).
export function useObservatoryData<T>(route: string, extra: string[] = []) {
  const { dashboard } = useDashboardContext();
  const params = [
    `code=${dashboard.params.code}`,
    `type=${dashboard.params.type}`,
    `year=${dashboard.params.year}`,
    ...extra,
  ];
  const { data, error, loading } = useApi<T>(GetApiUrl(route, params));
  return {
    data,
    error,
    loading,
    period: dashboard.params.period,
    type: dashboard.params.type,
  };
}
