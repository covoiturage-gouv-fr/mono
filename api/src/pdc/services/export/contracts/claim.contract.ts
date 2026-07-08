export type ParamsInterface = { targets: string[] };
export type ResultInterface =
  | { uuid: string; target: string; params: unknown }
  | null;

export const handlerConfig = {
  service: "exports",
  method: "claim",
} as const;

export const signature = `${handlerConfig.service}:${handlerConfig.method}` as const;
