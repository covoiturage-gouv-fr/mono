import { handler, KernelInterfaceResolver } from "../../../../ilos/common/index.ts";
import { Action as AbstractAction } from "../../../../ilos/core/index.ts";
import { logger } from "../../../../lib/logger/index.ts";
import { internalOnlyMiddlewares } from "../../../providers/middleware/index.ts";
import {
  ParamsInterface as GeoParamsInterface,
  ResultInterface as GeoResultInterface,
  signature as geoSignature,
} from "../../territory/contracts/findGeoBySiren.contract.ts";
import { PolicyStatusEnum } from "../contracts/common/interfaces/PolicyInterface.ts";
import { handlerConfig, ParamsInterface, ResultInterface } from "../contracts/simulateOnPastGeo.contract.ts";
import { alias } from "../contracts/simulateOnPastGeo.schema.ts";
import { MetadataStore } from "../engine/entities/MetadataStore.ts";
import { Policy } from "../engine/entities/Policy.ts";
import { SerializedPolicyInterface, TripRepositoryProviderInterfaceResolver } from "../interfaces/index.ts";
import { MemoryMetadataRepository } from "../providers/MemoryMetadataRepositoryProvider.ts";

@handler({
  ...handlerConfig,
  middlewares: [["validate", alias], ...internalOnlyMiddlewares("user")],
})
export class SimulateOnPastByGeoAction extends AbstractAction {
  private readonly DEFAULT_TIME_FRAME_6_MONTHS = 6;

  constructor(
    private kernel: KernelInterfaceResolver,
    private tripRepository: TripRepositoryProviderInterfaceResolver,
  ) {
    super();
  }

  public override async handle(params: ParamsInterface): Promise<ResultInterface> {
    // 0 Find related com
    const geoResult: GeoResultInterface = await this.findGeoBySiren(params);

    const today = new Date();
    const start_date = new Date();
    start_date.setMonth(
      today.getMonth() - (params.months || this.DEFAULT_TIME_FRAME_6_MONTHS),
    );

    // 1. Create a fake deserialized policy
    const policyTemplate: SerializedPolicyInterface = {
      start_date: start_date,
      end_date: today,
      _id: 1000,
      name: "",
      tz: "Europe/Paris",
      status: PolicyStatusEnum.ACTIVE,
      handler: params.policy_template_id,
      incentive_sum: 0n,
      territory_id: 0,
      max_amount: 10_000_000_00n,
      territory_selector: {
        ...(params.territory_insee === geoResult.aom_siren &&
          { aom: [geoResult.aom_siren] }),
        ...(params.territory_insee === geoResult.dep_siren &&
          { dep: [geoResult.dep_siren] }),
        ...(params.territory_insee === geoResult.epci_siren &&
          { epci: [geoResult.epci_siren] }),
        ...(params.territory_insee === geoResult.reg_siren &&
          { reg: [geoResult.reg_siren] }),
      },
    };

    // 2. Find selector and instanciate policy
    const policy = await Policy.import(policyTemplate);

    // 3. Start a cursor to find trips
    const cursor = this.tripRepository.findTripByGeo(
      geoResult.coms.map((m) => m.insee),
      policy.start_date,
      policy.end_date,
    );
    let done = false;

    let carpool_subsidized = 0;
    let amount = 0;

    const store = new MetadataStore(new MemoryMetadataRepository());
    do {
      const results = await cursor.next();
      done = !!results.done;
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

    return {
      trip_subsidized: carpool_subsidized,
      amount,
    };
  }

  private async findGeoBySiren(params: ParamsInterface) {
    const geoParamsInterface: GeoParamsInterface = {
      siren: params.territory_insee,
    };
    const geoResult: GeoResultInterface = await this.kernel.call<
      GeoParamsInterface
    >(geoSignature, geoParamsInterface, {
      call: {
        user: { permissions: ["common.territory.list"] },
      },
      channel: {
        service: handlerConfig.service,
      },
    });

    if (geoResult.coms.length === 0) {
      throw Error(
        `Could not find any coms for territory_insee ${params.territory_insee}`,
      );
    }
    return geoResult;
  }
}
