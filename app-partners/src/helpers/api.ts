import { Config } from "../config";

export const getApiUrl = (version: string, path: string) => {
  const host = Config.get<string>("next.public_api_url", "");
  return `${host}/${version}/${path}`;
};
