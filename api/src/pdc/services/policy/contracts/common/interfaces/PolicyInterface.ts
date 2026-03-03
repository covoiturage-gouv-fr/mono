import { Timezone } from "@/pdc/providers/validator/types.ts";
import { BoundedSlices, UnboundedSlices } from "./Slices.ts";

export enum PolicyStatusEnum {
  ACTIVE = "active",
  DRAFT = "draft",
  FINISHED = "finished",
}

export const policyStatusValues: PolicyStatusEnum[] = Object.values(
  PolicyStatusEnum,
);

export interface PolicyInterface {
  _id: number;
  territory_id: number;
  name: string;
  description: string;
  start_date: Date;
  end_date: Date;
  status: PolicyStatusEnum;
  handler: string;
  incentive_sum: bigint;
  params: {
    tz?: Timezone;
    slices?: BoundedSlices | UnboundedSlices;
    operators?: Array<string>;
    allTimeOperators?: Array<string>;
    limits?: {
      glob?: bigint;
    };
    booster_dates?: Array<string>;
    extras?: unknown;
  };
}
