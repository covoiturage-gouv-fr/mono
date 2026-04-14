import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { Infer } from "@/lib/superstruct/index.ts";
import { Direction } from "@/pdc/providers/superstruct/shared/index.ts";
import { JourneysByDistances } from "@/pdc/services/observatory/dto/distribution/JourneysByDistances.ts";
import { DistributionRepositoryInterfaceResolver } from "@/pdc/services/observatory/interfaces/DistributionRepositoryProviderInterface.ts";
export type ResultInterface = {
  code: string;
  libelle: string;
  direction: Infer<typeof Direction>;
  distances: {
    dist_classes: string;
    journeys: number;
  }[];
}[];

@handler({
  service: "observatory",
  method: "journeysByDistances",
  middlewares: [[
    "validate",
    JourneysByDistances,
  ]],
  apiRoute: {
    path: "/observatory/journeys-by-distances",
    method: "GET",
    public: true,
  },
})
export class JourneysByDistancesAction extends AbstractAction {
  constructor(private repository: DistributionRepositoryInterfaceResolver) {
    super();
  }

  public async handle(params: JourneysByDistances): Promise<ResultInterface> {
    return this.repository.getJourneysByDistances(params);
  }
}
