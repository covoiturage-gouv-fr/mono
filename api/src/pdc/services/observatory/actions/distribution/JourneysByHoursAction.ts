import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { Infer } from "@/lib/superstruct/index.ts";
import { Direction } from "@/pdc/providers/superstruct/shared/index.ts";
import { JourneysByHours } from "@/pdc/services/observatory/dto/distribution/JourneysByHours.ts";
import { DistributionRepositoryInterfaceResolver } from "../../interfaces/DistributionRepositoryProviderInterface.ts";
export type ResultInterface = {
  code: string;
  libelle: string;
  direction: Infer<typeof Direction>;
  hours: [
    {
      hour: number;
      journeys: number;
    },
  ];
}[];

@handler({
  service: "observatory",
  method: "journeysByHours",
  middlewares: [[
    "validate",
    JourneysByHours,
  ]],
  apiRoute: {
    path: "/observatory/journeys-by-hours",
    method: "GET",
    public: true,
  },
})
export class JourneysByHoursAction extends AbstractAction {
  constructor(private repository: DistributionRepositoryInterfaceResolver) {
    super();
  }

  public async handle(params: JourneysByHours): Promise<ResultInterface> {
    return this.repository.getJourneysByHours(params);
  }
}
