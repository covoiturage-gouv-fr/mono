import { BoundedSlices, UnboundedSlices } from "./Slices";

export enum PolicyStatusEnum {
  ACTIVE = "active",
  DRAFT = "draft",
  TEMPLATE = "template",
  FINISHED = "finished",
}

export const policyStatusValues: PolicyStatusEnum[] = Object.values(PolicyStatusEnum);

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
    slices?: BoundedSlices | UnboundedSlices;
    operators?: Array<string>;
    limits?: {
      glob?: number;
    };
    booster_dates?: Array<string>;
  };
}
