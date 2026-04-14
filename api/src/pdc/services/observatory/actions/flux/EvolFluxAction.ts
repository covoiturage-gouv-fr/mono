import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { EvolFlux } from "@/pdc/services/observatory/dto/flux/EvolFlux.ts";
import { FluxRepositoryInterfaceResolver } from "@/pdc/services/observatory/interfaces/FluxRepositoryProviderInterface.ts";
export type ResultInterface = {
  territory: EvolFlux["code"];
  l_territory: string;
  journeys?: number;
  passengers?: number;
  has_incentive?: number;
  distance?: number;
  duration?: number;
}[];

@handler({
  service: "observatory",
  method: "getEvolFlux",
  middlewares: [[
    "validate",
    EvolFlux,
  ]],
  apiRoute: {
    path: "/observatory/evol-flux",
    method: "GET",
    public: true,
  },
})
export class EvolFluxAction extends AbstractAction {
  constructor(private repository: FluxRepositoryInterfaceResolver) {
    super();
  }

  public async handle(params: EvolFlux): Promise<ResultInterface> {
    return this.repository.getEvolFlux(params);
  }
}
