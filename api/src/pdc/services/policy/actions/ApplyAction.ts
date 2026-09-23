import { defaultTimezone } from "@/config/time.ts";
import { ContextType, handler, InvalidParamsException, NotFoundException } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { env_or_false } from "@/lib/env/index.ts";
import { getPerformanceTimer, logger } from "@/lib/logger/index.ts";
import { internalOnlyMiddlewares } from "@/pdc/providers/middleware/index.ts";
import { isAfter, max as maxDate, min as minDate } from "dep:date-fns";
import { handlerConfig, ParamsInterface, ResultInterface } from "../contracts/apply.contract.ts";
import { alias } from "../contracts/apply.schema.ts";
import { Policy } from "../engine/entities/Policy.ts";
import { subDaysTz, today, toTzString } from "../helpers/index.ts";
import {
  IncentiveRepositoryProviderInterfaceResolver,
  PolicyRepositoryProviderInterfaceResolver,
  PolicyTerritoryRepositoryProviderInterfaceResolver,
  StatelessIncentiveInterface,
  TripRepositoryProviderInterfaceResolver,
} from "../interfaces/index.ts";

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
    private policyTerritoryRepository: PolicyTerritoryRepositoryProviderInterfaceResolver,
  ) {
    super();
  }

  public async handle(params: ParamsInterface): Promise<ResultInterface> {
    if (env_or_false("APP_DISABLE_POLICY_PROCESSING")) {
      return logger.warn(
        "[campaign:apply] policy processing is disabled by APP_DISABLE_POLICY_PROCESSING",
      );
    }

    const { from, to, override } = this.defaultParams(params);
    logger.info(`[campaign:apply] processing policy ${params.policy_id}`);
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

    pol.territories = await this.policyTerritoryRepository.findByPolicy(policy_id);
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

    logger.info(`[policy ${policy_id}] starting from ${s} to ${e}`);

    const batchSize = 1000;
    const cursor = this.tripRepository.findTripByPolicy(
      policy,
      start,
      end,
      batchSize,
      override,
    );
    let done = false;

    do {
      if (total === 0) {
        logger.info(`[policy ${policy._id}] Fetching carpools...`);
      }
      const incentives: Array<StatelessIncentiveInterface> = [];
      const results = await cursor.next();
      done = !!results.done;

      const bs = new Date(); // benchmark start
      if (results.value) {
        for (const carpool of results.value) {
          // 3. For each trip, process
          counter++;
          incentives.push(await policy.processStateless(carpool));
        }
      }

      // benchmark
      const ms = new Date().getTime() - bs.getTime();
      if (counter) {
        const rate = ((counter / ms) * 1000).toFixed(0);
        logger.info(
          `[policy ${policy._id}] ${counter} (${total}) trips done in ${ms}ms (${rate}/s)`,
        );
      }
      total += counter;
      counter = 0;

      // 4. Save incentives
      if (incentives.length) {
        const timer = getPerformanceTimer();
        await this.incentiveRepository.createOrUpdateMany(
          incentives.map((i) => i.export()),
        );
        logger.info(
          `[policy ${policy_id}] stored ${incentives.length} incentives in ${timer.stop()} ms`,
        );
      }
    } while (!done);

    logger.info(
      `[policy ${policy_id}] finished - ${total} in ${new Date().getTime() - bench.getTime()}ms`,
    );
  }
}
