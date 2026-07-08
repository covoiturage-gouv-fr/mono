export type ParamsInterface = { uuid: string; file_size: number };
export type ResultInterface = { status: string };

export const handlerConfig = {
  service: "exports",
  method: "complete",
} as const;

export const signature = `${handlerConfig.service}:${handlerConfig.method}` as const;
