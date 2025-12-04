import { Timezone } from "../../../../providers/validator/index.ts";
import { PolicyStatusEnum } from "../../contracts/common/interfaces/PolicyInterface.ts";
import {
  BoundedSlices,
  CarpoolInterface,
  MetadataAccessorInterface,
  MetadataRegistryInterface,
  MetadataStoreInterface,
  OperatorsEnum,
  SerializedIncentiveInterface,
  SliceInterface,
  StatefulIncentiveInterface,
  StatelessIncentiveInterface,
  TerritorySelectorsInterface,
  UnboundedSlices,
} from "../index.ts";

export interface PolicyInterface {
  _id: number;
  territory_id: number;
  territory_selector: TerritorySelectorsInterface;
  name: string;
  start_date: Date;
  end_date: Date;
  tz: Timezone;
  handler: PolicyHandlerInterface;
  status: PolicyStatusEnum;

  export(): SerializedPolicyInterface;
  processStateless(
    carpool: CarpoolInterface,
  ): Promise<StatelessIncentiveInterface>;
  processStateful(
    store: MetadataStoreInterface,
    incentive: SerializedIncentiveInterface,
  ): Promise<StatefulIncentiveInterface>;
  params(): PolicyHandlerParamsInterface;
  describe(): string;
}

export interface SerializedPolicyInterface {
  _id: number;
  territory_id: number;
  territory_selector: TerritorySelectorsInterface;
  name: string;
  descriptive_sheet_url?: string;
  start_date: Date;
  end_date: Date;
  tz: Timezone;
  handler: string;
  status: PolicyStatusEnum;
  incentive_sum: bigint;
  max_amount: bigint;
}

export interface PolicyHandlerStaticInterface {
  readonly id: string;
  readonly tz?: Timezone;
  readonly boosterDates?: string[];
  mode?<T>(date: Date, ...args: T[] | unknown[]): T;
  /**
   * Optional max amount to spend for the policy
   */
  new (max_amount: bigint): PolicyHandlerInterface;
}

export interface PolicyHandlerParamsInterface {
  tz?: Timezone;
  slices?: RunnableSlices | BoundedSlices;
  operators?: Array<OperatorsEnum>;
  allTimeOperators?: Array<OperatorsEnum>;
  limits?: { glob?: bigint };
  booster_dates?: Array<string>;
  extras?: unknown;
}

export interface PolicyHandlerInterface {
  max_amount?: bigint;
  load(): Promise<void>;
  processStateless(context: StatelessContextInterface): void;
  processStateful(context: StatefulContextInterface): void;
  params(): PolicyHandlerParamsInterface;
  describe(): string;
  getOperators?(datetime?: Date): OperatorsEnum[]; // TODO generalise this from GrandPoitiers campaign
}

export type StatelessRuleHelper<P> = (
  ctx: StatelessContextInterface,
  params: P,
) => boolean;

export interface StatefulContextInterface {
  incentive: StatefulIncentiveInterface;
  meta: MetadataAccessorInterface;
}

export interface StatelessContextInterface {
  incentive: StatelessIncentiveInterface;
  meta: MetadataRegistryInterface;
  carpool: CarpoolInterface;
  policy_territory_selector?: TerritorySelectorsInterface;
  policy_max_amount?: bigint;
}

/**
 * Extend the public bounded and unbounded slices with a runnable function
 * for use in the policy calculations
 */
export type RunnableSlice<TFunction> = SliceInterface & { fn: TFunction };
export type RunnableSlices =
  | BoundedSlices<RunnableSlice<(ctx: StatelessContextInterface) => number>>
  | UnboundedSlices<RunnableSlice<(ctx: StatelessContextInterface) => number>>;
