import { defaultTimezone } from "@/config/time.ts";
import { ContextType, handler, InvalidParamsException, NotFoundException } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { env_or_false } from "@/lib/env/index.ts";
import { getPerformanceTimer } from "@/lib/logger/index.ts";
import { errorCode, taggedLogger } from "@/lib/logger/tagged.ts";
import { internalOnlyMiddlewares } from "@/pdc/providers/middleware/index.ts";
import { isAfter, max as maxDate, min as minDate } from "dep:date-fns";
import { handlerConfig, ParamsInterface, ResultInterface } from "../contracts/apply.contract.ts";
import { alias } from "../contracts/apply.schema.ts";
import { Policy } from "../engine/entities/Policy.ts";
import { subDaysTz, today, toTzString } from "../helpers/index.ts";
import {
  IncentiveRepositoryProviderInterfaceResolver,
  PolicyRepositoryProviderInterfaceResolver,
  StatelessIncentiveInterface,
  TerritoryRepositoryProviderInterfaceResolver,
  TripRepositoryProviderInterfaceResolver,
} from "../interfaces/index.ts";

const log = taggedLogger("[campaign]");

@handler({
  ...handlerConfig,
  middlewares: [...internalOnlyMiddlewares(handlerConfig.service), [
    "validate",
    alias,
  ]],
})
export class ApplyAction extends AbstractAction {
  private readonly context: ContextType = {
    call: {
      user: {},
    },
    channel: {
      service: handlerConfig.service,
    },
  };

  constructor(
    private policyRepository: PolicyRepositoryProviderInterfaceResolver,
    private incentiveRepository: IncentiveRepositoryProviderInterfaceResolver,
    private tripRepository: TripRepositoryProviderInterfaceResolver,
    private territoryRepository: TerritoryRepositoryProviderInterfaceResolver,
  ) {
    super();
  }

  public async handle(params: ParamsInterface): Promise<ResultInterface> {
    if (env_or_false("APP_DISABLE_POLICY_PROCESSING")) {
      return log.warn("apply disabled", { env: "APP_DISABLE_POLICY_PROCESSING" });
    }

    const { from, to, override } = this.defaultParams(params);
    log.info("apply policy", { policy_id: params.policy_id });
    await this.processPolicy(params.policy_id, from, to, override);
  }

  /**
   * Incentives are calculated for the last week (from -7 days til today).
   * Finalization will be applied until the last 5 days
   */
  protected defaultParams(params: ParamsInterface): Required<ParamsInterface> {
    const tz = params.tz ?? defaultTimezone;

    return {
      tz,
      policy_id: params.policy_id,
      from: params.from ?? subDaysTz(today(tz), 7),
      to: params.to ?? today(tz),
      override: !!params.override,
    };
  }

  protected async processPolicy(
    policy_id: number,
    from: Date,
    to: Date,
    override = false,
  ): Promise<void> {
    // 1. Find policy
    const pol = await this.policyRepository.find(policy_id);
    if (!pol) {
      throw new NotFoundException(`[policy ${policy_id}] Not found`);
    }

    pol.perimeters = await this.territoryRepository.findPerimeters(pol.territory_id);
    const policy = await Policy.import(pol);

    // init counter and benchmark
    const bench = new Date();
    let total = 0;
    let counter = 0;

    // 2. Start a cursor to find trips
    // handle date boundaries
    const start = maxDate([from, policy.start_date]);
    const end = minDate([to, policy.end_date]);
    const s = toTzString(start);
    const e = toTzString(end);
    const pe = toTzString(policy.end_date);

    // throw if campaign start date is after policy end date
    if (isAfter(start, policy.end_date)) {
      throw new InvalidParamsException(
        `[policy ${policy._id}] 'from' (${s}) is after policy end_date (${pe})`,
      );
    }

    // throw if no time span
    if (end.getTime() - start.getTime() < 1) {
      throw new InvalidParamsException(
        `[policy ${policy._id}] Invalid time span (from ${s} to ${e})`,
      );
    }

    log.info("apply start", { policy_id, from: start.toISOString(), to: end.toISOString() });

    const batchSize = 1000;
    const cursor = this.tripRepository.findTripByPolicy(
      policy,
      start,
      end,
      batchSize,
      override,
    );
    let done = false;
    let failed = 0;

    try {
      do {
        if (total === 0) {
          log.debug("apply fetch carpools", { policy_id });
        }
        const incentives: Array<StatelessIncentiveInterface> = [];
        const results = await cursor.next();
        done = !!results.done;

        const bs = new Date(); // benchmark start
        if (results.value) {
          for (const carpool of results.value) {
            // 3. For each trip, process
            counter++;
            try {
              incentives.push(await policy.processStateless(carpool));
            } catch (e) {
              // no incentive stored: the trip is retried on the next run
              failed++;
              log.error("process stateless failed", {
                policy_id,
                operator_id: carpool.operator_id,
                operator_journey_id: carpool.operator_journey_id,
                error: errorCode(e),
              });
            }
          }
        }

        // benchmark
        const ms = new Date().getTime() - bs.getTime();
        if (counter) {
          const rate = ((counter / ms) * 1000).toFixed(0);
          log.debug("apply trips done", { policy_id, count: counter, total, ms, rate: Number(rate) });
        }
        total += counter;
        counter = 0;

        // 4. Save incentives
        if (incentives.length) {
          const timer = getPerformanceTimer();
          await this.incentiveRepository.createOrUpdateMany(
            incentives.map((i) => i.export()),
          );
          log.debug("apply stored incentives", { policy_id, count: incentives.length, ms: timer.stop() });
        }
      } while (!done);
    } finally {
      // releases the pg cursor and its pooled connection when the loop throws
      await cursor.return();
    }

    log.info("apply finished", { policy_id, count: total, failed, ms: new Date().getTime() - bench.getTime() });
  }
}
