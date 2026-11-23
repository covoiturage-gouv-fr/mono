import { defaultTimezone } from "@/config/time.ts";
import { ConfigInterfaceResolver, handler, KernelInterfaceResolver } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { env_or_false } from "@/lib/env/index.ts";
import { getPerformanceTimer } from "@/lib/logger/index.ts";
import { taggedLogger } from "@/lib/logger/tagged.ts";
import { internalOnlyMiddlewares } from "@/pdc/providers/middleware/index.ts";
import { handlerConfig, ParamsInterface, ResultInterface } from "../contracts/finalize.contract.ts";
import { alias } from "../contracts/finalize.schema.ts";
import { signature as syncincentivesumSignature } from "../contracts/syncIncentiveSum.contract.ts";
import { MetadataStore } from "../engine/entities/MetadataStore.ts";
import { Policy } from "../engine/entities/Policy.ts";
import { castUserStringToUTC, subDaysTz, today } from "../helpers/index.ts";
import {
  IncentiveRepositoryProviderInterfaceResolver,
  IncentiveStatusEnum,
  MetadataLifetime,
  MetadataRepositoryProviderInterfaceResolver,
  PolicyInterface,
  PolicyRepositoryProviderInterfaceResolver,
  StatefulIncentiveInterface,
} from "../interfaces/index.ts";

const log = taggedLogger("[campaign]");

// TOFIX ?
// from and to props must be strings to pass schema validation
// we need Dates in the code.
type DefaultParamsInterface = Omit<Required<ParamsInterface>, "from" | "to"> & {
  from: Date;
  to: Date;
};

@handler({
  ...handlerConfig,
  middlewares: [...internalOnlyMiddlewares(handlerConfig.service), [
    "validate",
    alias,
  ]],
})
export class FinalizeAction extends AbstractAction {
  constructor(
    private policyRepository: PolicyRepositoryProviderInterfaceResolver,
    private incentiveRepository: IncentiveRepositoryProviderInterfaceResolver,
    private metaRepository: MetadataRepositoryProviderInterfaceResolver,
    private kernel: KernelInterfaceResolver,
    private config: ConfigInterfaceResolver,
  ) {
    super();
  }

  public override async handle(params: ParamsInterface): Promise<ResultInterface> {
    if (env_or_false("APP_DISABLE_POLICY_PROCESSING")) {
      log.warn("finalize disabled", { env: "APP_DISABLE_POLICY_PROCESSING" });
      return;
    }

    const timer = getPerformanceTimer();
    const { from, to, sync_incentive_sum } = await this.defaultParams(params);

    // resync incentive_sum for all policies
    // call the action instead of the repo to avoid having
    // to duplicate the listing of all campaigns
    if (sync_incentive_sum) {
      log.info("finalize sync incentive_sum");
      await this.kernel.call(syncincentivesumSignature, {}, {
        channel: { service: handlerConfig.service },
      });
    }

    await this.finalizeRange(from, to);

    log.info("finalize done", { ms: timer.stop() });
  }

  public async finalizeRange(from: Date, to: Date): Promise<void> {
    const policyMap: Map<number, PolicyInterface> = new Map();

    const timespan = 6 * 3600000; // N hours in milliseconds
    let currentTime = from.getTime();
    const endTime = to.getTime();

    while (currentTime <= endTime) {
      const currentFrom = new Date(currentTime);
      const currentTo = new Date(currentTime + timespan);

      try {
        // Update incentive on canceled carpool
        const subtimer = getPerformanceTimer();
        await this.incentiveRepository.disableOnExcludedCarpool(currentFrom, currentTo);
        log.debug("finalize disable excluded carpool", { ms: subtimer.stop() });

        log.info("finalize stateful start", { from: currentFrom.toISOString(), to: currentTo.toISOString() });
        await this.processStatefulPolicies(policyMap, currentTo, currentFrom);
        log.debug("finalize stateful done");

        // Lock all
        log.debug("finalize set status");
        await this.incentiveRepository.setStatus(currentFrom, currentTo);
        log.debug("finalize lock done");
      } catch (e) {
        log.warn("finalize unlock incentives", { from: currentFrom.toISOString(), to: currentTo.toISOString() });
        await this.incentiveRepository.setStatus(currentFrom, currentTo, true);
        throw e;
      }

      currentTime += timespan; // move to the next hour
    }
  }

  /**
   * Trips are finalized until 5 days ago to make sure the data is sent by the operators
   * and make sure the trips are finalized before APDF are generated on the 6th of every month
   */
  protected async defaultParams(
    params: ParamsInterface,
  ): Promise<DefaultParamsInterface> {
    const tz = params.tz ?? defaultTimezone;
    const to = castUserStringToUTC(params.to) || subDaysTz(today(tz), this.config.get("policies.finalize.to"));
    const from = castUserStringToUTC(params.from) ||
      (await this.incentiveRepository.latestDraft()) ||
      subDaysTz(today(tz), this.config.get("policies.finalize.from"));

    return { tz, from, to, sync_incentive_sum: !!params.sync_incentive_sum };
  }

  protected async processStatefulPolicies(
    policyMap: Map<number, PolicyInterface>,
    to: Date,
    from?: Date,
  ): Promise<void> {
    // 0. Instanciate a meta store
    const store = new MetadataStore(this.metaRepository);
    // 1. Start a cursor to find incentives
    const cursor = this.incentiveRepository.findDraftIncentive(to, 100, from);
    let done = false;
    do {
      const bench = new Date().getTime();
      const updatedIncentives: StatefulIncentiveInterface[] = [];
      const results = await cursor.next();
      done = !!results.done;

      if (results.value) {
        for (const incentive of results.value) {
          if (!incentive.policy_id) {
            log.warn("finalize skip incentive without policy_id", { incentive_id: incentive._id });
            continue;
          }

          // 2. Get policy
          if (!policyMap.has(incentive.policy_id)) {
            const res = await this.policyRepository.find(incentive.policy_id);
            if (!res) {
              log.warn("finalize skip policy not found", { policy_id: incentive.policy_id });
              continue;
            }

            policyMap.set(incentive.policy_id, await Policy.import(res));
          }

          const policy = policyMap.get(incentive.policy_id);

          if (!policy) {
            log.warn("finalize skip policy not in map", { policy_id: incentive.policy_id });
            continue;
          }

          // 3. Process stateful rule
          updatedIncentives.push(await policy.processStateful(store, incentive));
        }
      }

      // 4. Update incentives
      await this.incentiveRepository.updateStatefulAmount(updatedIncentives, IncentiveStatusEnum.Pending);

      const duration = new Date().getTime() - bench;
      const len = updatedIncentives.length;
      const rate = ((len / duration) * 1000).toFixed(3);

      log.debug("finalize incentives done", { count: len, ms: duration, rate: Number(rate) });
    } while (!done);

    // 5. Persist meta
    log.debug("finalize store metadata");
    await store.store(MetadataLifetime.Day);
    log.debug("finalize store metadata done");
  }
}
