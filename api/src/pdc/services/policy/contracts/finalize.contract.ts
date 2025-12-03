import { Timezone } from "@/pdc/providers/validator/index.ts";

export type ParamsInterface = Partial<{
  to: string;
  from: string;
  tz: Timezone;
  sync_incentive_sum: boolean;
}>;

export type ResultInterface = void;

export const handlerConfig = {
  service: "campaign",
  method: "finalize",
} as const;

export const signature = `${handlerConfig.service}:${handlerConfig.method}` as const;
