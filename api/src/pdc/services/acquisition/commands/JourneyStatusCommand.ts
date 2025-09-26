import { command, CommandInterface } from "@/ilos/common/index.ts";
import { CarpoolStatusService } from "@/pdc/providers/carpool/providers/CarpoolStatusService.ts";

interface Options {
  operator_id: number;
  operator_journey_id: string;
}

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
    for (const operator_journey_id of list) {
      const result = await this.statusService.findByOperatorJourneyId(
        operator_id,
        operator_journey_id,
        "3.1",
      );

      if (!result) {
        console.log(`${operator_journey_id} not found`);
        continue;
      }

      console.log(this.statusService.castToStatusResult(operator_journey_id, result));
    }
  }
}
