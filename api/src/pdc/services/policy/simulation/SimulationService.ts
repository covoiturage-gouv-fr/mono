import { ConfigInterfaceResolver, KernelInterfaceResolver, provider } from "@/ilos/common/index.ts";
import { taggedLogger } from "@/lib/logger/tagged.ts";
import { TerritoryPerimeterInterface } from "@/pdc/services/territory/contracts/common/interfaces/TerritoryPerimeterInterface.ts";
import { parseTerritoryCodes } from "@/pdc/services/territory/helpers/perimeters.ts";
import { PerimeterRepositoryProviderInterfaceResolver } from "@/pdc/services/territory/interfaces/PerimeterRepositoryProviderInterface.ts";
import { ApplyAction } from "../actions/ApplyAction.ts";
import { FinalizeAction } from "../actions/FinalizeAction.ts";
import {
  PolicyRepositoryProviderInterfaceResolver,
  TerritoryRepositoryProviderInterfaceResolver,
  TripRepositoryProviderInterfaceResolver,
} from "../interfaces/index.ts";
import { SimulationRow } from "./csv.ts";
import { InMemoryIncentiveRepository } from "./InMemoryIncentiveRepository.ts";
import { InMemoryMetadataRepository } from "./InMemoryMetadataRepository.ts";
import { SimulationRepositoryProvider } from "./SimulationRepositoryProvider.ts";

const log = taggedLogger("[campaign]");
const FINALIZE_WINDOW_MS = 6 * 3600000;

export interface SimulationParams {
  policy_id: number;
  from: Date;
  to: Date;
  /** `type:code` list replacing the territory perimeter, same format as territory:perimeter */
  perimeter?: string;
}

/**
 * Replays campaign:apply --override then campaign:finalize on in-memory repositories.
 * The only database access is reads: no incentive, metadata, status or perimeter is written.
 */
@provider()
export class SimulationService {
  constructor(
    private policyRepository: PolicyRepositoryProviderInterfaceResolver,
    private tripRepository: TripRepositoryProviderInterfaceResolver,
    private territoryRepository: TerritoryRepositoryProviderInterfaceResolver,
    private perimeterRepository: PerimeterRepositoryProviderInterfaceResolver,
    private simulationRepository: SimulationRepositoryProvider,
    private kernel: KernelInterfaceResolver,
    private config: ConfigInterfaceResolver,
  ) {}

  async simulate({ policy_id, from, to, perimeter }: SimulationParams): Promise<SimulationRow[]> {
    const perimeters = perimeter ? await this.resolvePerimeters(perimeter) : undefined;

    const excluded = await this.simulationRepository.findExcludedCarpools(
      from,
      new Date(to.getTime() + FINALIZE_WINDOW_MS),
    );
    const incentives = new InMemoryIncentiveRepository(
      new Set(excluded.map((c) => `${c.operator_id}/${c.operator_journey_id}`)),
    );
    // empty on purpose: counters and envelope restart from zero
    const metadata = new InMemoryMetadataRepository();

    const trips = new Map<string, SimulationRow>();
    const apply = new ApplyAction(this.policyRepository, incentives, this.tripRepository, this.territoryRepository);
    await apply.processPolicy(policy_id, from, to, true, {
      perimeters,
      onTrip: (carpool, incentive, reason) => {
        trips.set(`${carpool.operator_id}/${carpool.operator_journey_id}`, {
          operator_id: carpool.operator_id,
          operator_journey_id: carpool.operator_journey_id,
          datetime: carpool.datetime,
          start_arr: carpool.start?.arr ?? "",
          end_arr: carpool.end?.arr ?? "",
          distance: carpool.distance,
          simulated_stateless_amount: incentive?.get(),
          rejection_reason: reason,
        });
      },
    });

    const finalize = new FinalizeAction(
      this.policyRepository,
      incentives,
      metadata,
      this.kernel,
      this.config,
    );
    await finalize.finalizeRange(from, to);

    for (const i of incentives.all()) {
      const row = trips.get(`${i.operator_id}/${i.operator_journey_id}`);
      if (row) {
        row.simulated_final_amount = i.statefulAmount;
        row.simulated_final_status = i.status;
      }
    }

    const current = await this.simulationRepository.findCurrentIncentives(policy_id, from, to);
    for (const c of current) {
      const row = trips.get(`${c.operator_id}/${c.operator_journey_id}`);
      if (row) {
        row.current_amount = c.amount;
        row.current_status = c.status;
      }
    }

    log.info("simulation done", { policy_id, trips: trips.size });
    return [...trips.values()].sort((a, b) => a.datetime.getTime() - b.datetime.getTime());
  }

  private async resolvePerimeters(codes: string): Promise<TerritoryPerimeterInterface[]> {
    const { arr, unknown } = await this.perimeterRepository.resolve(parseTerritoryCodes([codes]));
    if (unknown.length) {
      throw new Error(`Codes inconnus : ${unknown.join(" ")}`);
    }
    return [{ version: 1, arr, valid_from: new Date(0), valid_to: null }];
  }
}
