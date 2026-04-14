import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { BestFlux } from "@/pdc/services/observatory/dto/flux/BestFlux.ts";
import { FluxRepositoryInterfaceResolver } from "@/pdc/services/observatory/interfaces/FluxRepositoryProviderInterface.ts";
export type ResultInterface = {
  territory_1: BestFlux["code"];
  l_territory_1: string;
  territory_2: BestFlux["code"];
  l_territory_2: string;
  journeys: number;
}[];

@handler({
  service: "observatory",
  method: "getBestFlux",
  middlewares: [[
    "validate",
    BestFlux,
  ]],
  apiRoute: {
    path: "/observatory/best-flux",
    method: "GET",
    public: true,
  },
})
export class BestFluxAction extends AbstractAction {
  constructor(private repository: FluxRepositoryInterfaceResolver) {
    super();
  }

  public async handle(params: BestFlux): Promise<ResultInterface> {
    return this.repository.getBestFlux(params);
  }
}
