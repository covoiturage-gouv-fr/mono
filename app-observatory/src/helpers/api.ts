import { Config } from "@/config";
import { useDashboardContext } from "../context/DashboardProvider";

// Base de l'API observatoire (datalake) ; le path /v3/observatory est fixé ici, pas dans la var d'env.
export const OBSERVATORY_API_URL = `${Config.get<string>("next.public_datalake_base_url", "")}/v3/observatory`;

export const GetApiUrl = (
  route: string,
  params: string[],
) => {
  const { dashboard } = useDashboardContext();
  switch (dashboard.params.period) {
    case "month":
      params.push(`month=${dashboard.params.month}`);
      break;
    case "trimester":
      params.push(`trimester=${dashboard.params.trimester}`);
      break;
    case "semester":
      params.push(`semester=${dashboard.params.semester}`);
      break;
  }
  return `${OBSERVATORY_API_URL}/${route}?${params.join("&")}`;
};
