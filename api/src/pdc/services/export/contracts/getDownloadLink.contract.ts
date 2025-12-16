export type ParamsInterface = {
  id: string;
};

export type ResultItemInterface = {
  url: string;
};

export type ResultInterface = {
  meta: null;
  data: ResultItemInterface;
};

export const handlerConfig = {
  service: "exports",
  method: "getDownloadLink",
} as const;

export const signature = `${handlerConfig.service}:${handlerConfig.method}` as const;
