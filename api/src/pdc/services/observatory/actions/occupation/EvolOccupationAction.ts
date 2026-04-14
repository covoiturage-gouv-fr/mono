import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { EvolOccupation } from "@/pdc/services/observatory/dto/occupation/EvolOccupation.ts";
import { OccupationRepositoryInterfaceResolver } from "@/pdc/services/observatory/interfaces/OccupationRepositoryProviderInterface.ts";
export type ResultInterface = {
  territory: EvolOccupation["code"];
  l_territory: string;
  journeys?: number;
  trips?: number;
  has_incentive?: number;
  occupation_rate?: number;
}[];

@handler({
  service: "observatory",
  method: "getEvolOccupation",
  middlewares: [[
    "validate",
    EvolOccupation,
  ]],
  apiRoute: {
    path: "/observatory/evol-occupation",
    method: "GET",
    public: true,
  },
})
export class EvolOccupationAction extends AbstractAction {
  constructor(private repository: OccupationRepositoryInterfaceResolver) {
    super();
  }

  public async handle(params: EvolOccupation): Promise<ResultInterface> {
    return this.repository.getEvolOccupation(params);
  }
}
