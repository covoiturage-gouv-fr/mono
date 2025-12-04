import { RedisKey } from "dep:redis";
import { handler } from "../../../../ilos/common/index.ts";
import { RedisConnection } from "../../../../ilos/connection-redis/index.ts";
import { Action as AbstractAction } from "../../../../ilos/core/index.ts";
import { logger } from "../../../../lib/logger/index.ts";
import { copyGroupIdAndApplyGroupPermissionMiddlewares } from "../../../providers/middleware/index.ts";
import { PolicyStatusEnum } from "../contracts/common/interfaces/PolicyInterface.ts";
import { handlerConfig, ParamsInterface, ResultInterface } from "../contracts/simulateOnPast.contract.ts";
import { alias } from "../contracts/simulateOnPast.schema.ts";
import { MetadataStore } from "../engine/entities/MetadataStore.ts";
import { Policy } from "../engine/entities/Policy.ts";
import {
  SerializedPolicyInterface,
  TerritoryRepositoryProviderInterfaceResolver,
  TripRepositoryProviderInterfaceResolver,
} from "../interfaces/index.ts";

@handler({
  ...handlerConfig,
  middlewares: [
    ["validate", alias],
    ...copyGroupIdAndApplyGroupPermissionMiddlewares({
      territory: "territory.policy.simulate.past",
      registry: "registry.policy.simulate.past",
    }),
  ],
})
export class SimulateOnPastAction extends AbstractAction {
  private readonly TEN_DAYS_IN_SECONDS: number = 10 * 86400;

  constructor(
    private tripRepository: TripRepositoryProviderInterfaceResolver,
    private connection: RedisConnection,
    private territoryRepository: TerritoryRepositoryProviderInterfaceResolver,
  ) {
    super();
  }

  public async handle(params: ParamsInterface): Promise<ResultInterface> {
    const today = new Date();
    const start_date = new Date();
    start_date.setMonth(today.getMonth() - params.months);

    // 0. Returns Redis cache result for a given territory and month number if present
    const cachedResult: string = await this.connection.getClient().get(
      this.getSimulationCachingKey(params),
    );
    if (cachedResult) {
      logger.debug(
        `[policy] Found cached policy simulation for territory ${params.territory_id} and ${params.months} months`,
      );
      return JSON.parse(cachedResult);
    }

    // 1. Find selector and instanciate policy
    const territory_selector = await this.territoryRepository
      .findSelectorFromId(params.territory_id);
    const serialized_policy: SerializedPolicyInterface = {
      ...params,
      status: PolicyStatusEnum.ACTIVE,
      start_date,
      end_date: today,
      tz: "Europe/Paris",
      incentive_sum: 0n,
      territory_selector,
      max_amount: 10_000_000_00n,
      _id: 1,
    };
    const policy = await Policy.import(serialized_policy);
    // 2. Start a cursor to find trips
    const cursor = this.tripRepository.findTripByPolicy(
      policy,
      policy.start_date,
      policy.end_date,
    );
    let done = false;

    let carpool_subsidized = 0;
    let amount = 0;

    const store = new MetadataStore();
    do {
      const results = await cursor.next();
      done = results.done;
      if (results.value) {
        for (const carpool of results.value) {
          // 3. For each trip, process stateless and stateful safely
          try {
            const incentive = await policy.processStateless(carpool);
            const finalIncentive = await policy.processStateful(
              store,
              incentive.export(),
            );
            const finalAmount = finalIncentive.get();
            if (finalAmount > 0) {
              carpool_subsidized += 1;
            }
            amount += finalAmount;
          } catch (e) {
            logger.error(e);
          }
        }
      }
    } while (!done);

    const result: ResultInterface = {
      start_date,
      end_date: today,
      trip_subsidized: carpool_subsidized,
      amount,
    };
    this.cacheSimultionResult(params, result);
    return result;
  }

  private cacheSimultionResult(
    params: ParamsInterface,
    result: ResultInterface,
  ): void {
    this.connection
      .getClient()
      .set(
        this.getSimulationCachingKey(params),
        JSON.stringify(result),
        "EX",
        this.TEN_DAYS_IN_SECONDS,
      );
  }

  private getSimulationCachingKey(params: ParamsInterface): RedisKey {
    return `simulations:${params.territory_id}:${params.months}`;
  }
}
