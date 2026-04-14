import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { BestTerritories } from "@/pdc/services/observatory/dto/occupation/BestTerritories.ts";
import { OccupationRepositoryInterfaceResolver } from "@/pdc/services/observatory/interfaces/OccupationRepositoryProviderInterface.ts";
import { isPublished } from "../../helpers/publishedDate.ts";
export type ResultInterface = {
  territory: BestTerritories["code"];
  l_territory: string;
  journeys: number;
}[];

@handler({
  service: "observatory",
  method: "getBestTerritories",
  middlewares: [[
    "validate",
    BestTerritories,
  ]],
  apiRoute: {
    path: "/observatory/best-territories",
    method: "GET",
    public: true,
  },
})
export class BestTerritoriesAction extends AbstractAction {
  constructor(private repository: OccupationRepositoryInterfaceResolver) {
    super();
  }

  public async handle(params: BestTerritories): Promise<ResultInterface> {
    if (!isPublished(params)) return [];
    return this.repository.getBestTerritories(params);
  }
}
