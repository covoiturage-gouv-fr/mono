import {
  IncentiveStateEnum,
  IncentiveStatusEnum,
  MetadataRegistryInterface,
  SerializedIncentiveInterface,
  StatefulIncentiveInterface,
  StatelessIncentiveInterface,
} from "../../interfaces/index.ts";
import { MetadataRegistry } from "./MetadataRegistry.ts";

export class Incentive<T> {
  constructor(
    public readonly _id: T,
    public readonly policy_id: number,
    public readonly operator_id: number,
    public readonly operator_journey_id: string,
    public readonly datetime: Date,
    public statelessAmount: number,
    public statefulAmount: number,
    public status: IncentiveStatusEnum,
    public state: IncentiveStateEnum,
    public meta: MetadataRegistryInterface,
  ) {}

  static create(
    policy_id: number,
    operator_id: number,
    operator_journey_id: string,
    datetime: Date,
    meta?: MetadataRegistryInterface,
  ): Incentive<undefined> {
    return new StatelessIncentive(
      undefined,
      policy_id,
      operator_id,
      operator_journey_id,
      datetime,
      0,
      0,
      IncentiveStatusEnum.Draft,
      IncentiveStateEnum.Regular,
      meta || MetadataRegistry.create(policy_id, datetime),
    );
  }

  static import(data: SerializedIncentiveInterface): Incentive<number> {
    return new Incentive(
      data._id,
      data.policy_id,
      data.operator_id,
      data.operator_journey_id,
      data.datetime,
      data.statelessAmount,
      data.statefulAmount,
      data.status,
      data.state,
      MetadataRegistry.import(data.policy_id, data.datetime, data.meta),
    );
  }

  export(): SerializedIncentiveInterface<T> {
    return {
      _id: this._id,
      policy_id: this.policy_id,
      operator_id: this.operator_id,
      operator_journey_id: this.operator_journey_id,
      datetime: this.datetime,
      statelessAmount: this.statelessAmount,
      statefulAmount: this.statefulAmount,
      status: this.status,
      state: this.state,
      meta: this.meta.export(),
    };
  }

  get(): number {
    return this._id ? this.statefulAmount : this.statelessAmount;
  }

  set(amount: number): void {
    const intAmount = Math.round(amount);
    if (this._id) {
      this.statefulAmount = intAmount;
    } else {
      this.statelessAmount = intAmount;
      this.statefulAmount = intAmount;
    }
  }

  getMeta(): MetadataRegistryInterface {
    return this.meta;
  }

  setMeta(registry: MetadataRegistryInterface): void {
    this.meta = registry;
  }
}

export class StatelessIncentive extends Incentive<undefined> implements StatelessIncentiveInterface {
  // @ts-ignore undefined vs. unknown
  override export(): SerializedIncentiveInterface<undefined> {
    return super.export();
  }
}

export class StatefulIncentive extends Incentive<number> implements StatefulIncentiveInterface {
  override export(): SerializedIncentiveInterface<number> {
    return super.export();
  }
}
