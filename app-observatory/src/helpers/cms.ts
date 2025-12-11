import { Config } from "@/config";
import { cms } from "@/config/cms";
import qs from "qs";

export const cmsHost = Config.get<string>("cms.host");
export const cmsActusByPage = Config.get<number>("cms.actusByPage");
export const cmsRessourcesByPage = Config.get<number>("cms.ressourcesByPage");

export const shorten = (str: string, maxLen: number, separator = " ", end = "...") => {
  if (str.length <= maxLen) return str;
  return `${str.substring(0, str.lastIndexOf(separator, maxLen))} ${end}`;
};

export const getNbPages = (total: number, max: number) => {
  return total / max > 1 ? Math.round(total / max) : 1;
};

export const fetchAPI = async (path: string, urlParamsObject = {}, options = {}) => {
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
    const data = await response.json();
    return data;
  } catch (e) {
    console.error(e);
    throw new Error(`Please check if your server is running and you set all the required tokens.`);
  }
};
