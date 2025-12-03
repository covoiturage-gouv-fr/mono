import { MetadataRegistryInterface, SerializedMetadataVariableDefinitionInterface } from "../index.ts";

export enum IncentiveStateEnum {
  Regular = "regular",
  Null = "null",
  Disabled = "disabled",
}

export enum IncentiveStatusEnum {
  Draft = "draft",
  Pending = "pending",
  Validated = "validated",
  Warning = "warning",
  Error = "error",
}

export interface SerializedIncentiveInterface<T = number> {
  _id: T;
  policy_id: number;
  operator_id: number;
  operator_journey_id: string;
  datetime: Date;
  statelessAmount: number;
  statefulAmount: number;
  status: IncentiveStatusEnum;
  state: IncentiveStateEnum;
  meta: Array<SerializedMetadataVariableDefinitionInterface>;
}

export interface CommonIncentiveInterface {
  get(): number;
  set(amount: number): void;
}

export interface StatelessIncentiveInterface extends CommonIncentiveInterface {
  export<T = unknown>(): SerializedIncentiveInterface<T>;
  setMeta(registry: MetadataRegistryInterface): void;
}

export interface StatefulIncentiveInterface extends CommonIncentiveInterface {
  export(): SerializedIncentiveInterface<number>;
  getMeta(): MetadataRegistryInterface;
}
