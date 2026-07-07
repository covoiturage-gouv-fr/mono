export type ParamsInterface = { uuid: string; message: string };
export type ResultInterface = { status: string };

export const handlerConfig = {
  service: "exports",
  method: "fail",
} as const;

export const signature = `${handlerConfig.service}:${handlerConfig.method}` as const;
