import { logger } from "../../../../../lib/logger/index.ts";
import { Timezone } from "../../../../providers/validator/index.ts";
import { PolicyStatusEnum } from "../../contracts/common/interfaces/PolicyInterface.ts";
import {
  CarpoolInterface,
  MetadataStoreInterface,
  PolicyHandlerInterface,
  PolicyHandlerParamsInterface,
  PolicyHandlerStaticInterface,
  PolicyInterface,
  SerializedIncentiveInterface,
  SerializedPolicyInterface,
  StatefulIncentiveInterface,
  StatelessContextInterface,
  StatelessIncentiveInterface,
  TerritorySelectorsInterface,
} from "../../interfaces/index.ts";
import { NotEligibleTargetException } from "../exceptions/NotEligibleTargetException.ts";
import { UnknownHandlerException } from "../exceptions/UnknownHandlerException.ts";
import { isSelected } from "../helpers/index.ts";
import { policies } from "../policies/index.ts";
import { StatefulContext, StatelessContext } from "./Context.ts";

export class Policy implements PolicyInterface {
  constructor(
    public _id: number,
    public territory_id: number,
    public territory_selector: TerritorySelectorsInterface,
    public name: string,
    public start_date: Date,
    public end_date: Date,
    public tz: Timezone,
    public handler: PolicyHandlerInterface,
    public status: PolicyStatusEnum,
    public incentive_sum: bigint,
    public descriptive_sheet_url?: string,
  ) {}

  static async import(data: SerializedPolicyInterface): Promise<Policy> {
    const ctor = policies.get(data.handler);
    if (!ctor) {
      throw new UnknownHandlerException();
    }

    const hler = new ctor(data.max_amount);
    await hler.load();

    const pcy = new Policy(
      data._id,
      data.territory_id,
      data.territory_selector,
      data.name,
      data.start_date,
      data.end_date,
      data.tz,
      hler,
      data.status,
      data.incentive_sum,
      data.descriptive_sheet_url,
    );

    return pcy;
  }

  export(): SerializedPolicyInterface {
    return {
      _id: this._id,
      territory_id: this.territory_id,
      territory_selector: this.territory_selector,
      name: this.name,
      descriptive_sheet_url: this.descriptive_sheet_url,
      start_date: this.start_date,
      end_date: this.end_date,
      tz: this.tz,
      status: this.status,
      incentive_sum: this.incentive_sum,
      handler: (this.handler.constructor as PolicyHandlerStaticInterface).id,
      max_amount: this.handler.max_amount!,
    };
  }

  async processStateless(
    carpool: CarpoolInterface,
  ): Promise<StatelessIncentiveInterface> {
    const context: StatelessContextInterface = StatelessContext.fromCarpool(
      this._id,
      carpool,
    );
    context.policy_territory_selector = this.territory_selector;
    if (this.guard(carpool)) {
      try {
        this.handler.processStateless(context);
      } catch (e) {
        if (e instanceof NotEligibleTargetException) {
          context.incentive.set(0);
          return context.incentive;
        }
        logger.error(
          `Stateless incentive calculation for carpool ${carpool.operator_id} ${carpool.operator_journey_id} failed : ${e.message}`,
        );
        logger.debug(e);
        throw e;
      }
    }
    return context.incentive;
  }

  async processStateful(
    store: MetadataStoreInterface,
    incentive: SerializedIncentiveInterface,
  ): Promise<StatefulIncentiveInterface> {
    try {
      const context = await StatefulContext.fromIncentive(store, incentive);
      if (context.meta.isEmpty() || context.incentive.get() === 0) {
        return context.incentive;
      }
      this.handler.processStateful(context);
      await store.save(context.meta);
      return context.incentive;
    } catch (e) {
      logger.error(`Stateful incentive calculation failed for ${incentive._id}: ${e.message}`);
      logger.debug(e);
      throw e;
    }
  }

  protected guard(carpool: CarpoolInterface): boolean {
    if (carpool.datetime < this.start_date) {
      return false;
    }

    if (carpool.datetime > this.end_date) {
      return false;
    }

    if (
      !this.territory_selector ||
      Object.keys(this.territory_selector).length <= 0
    ) {
      return true;
    }

    if (
      !isSelected(carpool.start, this.territory_selector) &&
      !isSelected(carpool.end, this.territory_selector)
    ) {
      return false;
    }

    return true;
  }

  params(): PolicyHandlerParamsInterface {
    return this.handler.params();
  }
}
