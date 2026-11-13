import { Config } from "@/config";
import { cms } from "@/config/cms";
import { StrapiResponse } from "@/interfaces/cms/strapi";
import qs from "qs";

export const cmsHost = Config.get<string>("cms.host");
export const cmsActusByPage = Config.get<number>("cms.actusByPage");
export const cmsRessourcesByPage = Config.get<number>("cms.ressourcesByPage");

export { shorten } from "./text";

export const getNbPages = (total: number, max: number) => {
  return total / max > 1 ? Math.round(total / max) : 1;
};

export const fetchAPI = async <A>(path: string, urlParamsObject = {}, options = {}): Promise<StrapiResponse<A>> => {
  try {
    const mergedOptions = {
      next: cms.next,
      headers: cms.headers,
      ...options,
    };
    // Build request URL
    const queryString = qs.stringify(urlParamsObject);
    const requestUrl = `${cmsHost}/api${path}${queryString ? `?${queryString}` : ""}`;
    const response = await fetch(requestUrl, mergedOptions);
    return (await response.json()) as StrapiResponse<A>;
  } catch (e) {
    console.error(e);
    throw new Error(`Please check if your server is running and you set all the required tokens.`, { cause: e });
  }
};
