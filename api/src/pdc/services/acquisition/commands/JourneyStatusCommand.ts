import { command, CommandInterface, NotFoundException } from "@/ilos/common/index.ts";
import { CarpoolStatusService } from "@/pdc/providers/carpool/providers/CarpoolStatusService.ts";
import { lastApiVersion } from "@/pdc/proxy/config/API_VERSION.ts";

@command({
  signature: "journey:status <operator_id> <operator_journey_id...>",
  description: "Get the status of a journey",
  options: [],
})
export class JourneyStatusCommand implements CommandInterface {
  constructor(
    private statusService: CarpoolStatusService,
  ) {
  }

  public async call(id: string, list: string[]): Promise<void> {
    const operator_id = parseInt(id, 10);
    if (Number.isNaN(operator_id)) {
      console.error(`Invalid operator_id: "${id}". Must be a valid integer.`);
      return;
    }
    for (const operator_journey_id of list) {
      try {
        const result = await this.statusService.findByOperatorJourneyId(
          operator_id,
          operator_journey_id,
          lastApiVersion(),
        );

        const status = this.statusService.castToStatusResult(operator_journey_id, result);

        console.log(`${operator_journey_id} status=${status.status} journey_id=${status.journey_id}`);
      } catch (e) {
        if (e instanceof NotFoundException) {
          console.log(`${operator_journey_id} status=NOT_FOUND`);
          continue;
        }

        throw e;
      }
    }
  }
}
