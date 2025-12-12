import { SerializedPolicyInterface } from "../interfaces/engine/PolicyInterface.ts";

export interface ParamsInterface {
  _id: number;
  descriptive_sheet_url: string;
}

export type ResultInterface = SerializedPolicyInterface;

export const handlerConfig = {
  service: "policy",
  method: "updateDescriptiveSheetUrl",
} as const;

export const signature = `${handlerConfig.service}:${handlerConfig.method}` as const;
