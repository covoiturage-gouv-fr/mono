export type ParamsInterface = {
  id: string;
};

export type ResultInterface = {
  url: string;
};

export const handlerConfig = {
  service: "exports",
  method: "getDownloadLink",
} as const;

export const signature = `${handlerConfig.service}:${handlerConfig.method}` as const;
