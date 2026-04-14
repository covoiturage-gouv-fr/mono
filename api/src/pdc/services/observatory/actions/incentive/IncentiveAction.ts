import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { Infer } from "@/lib/superstruct/index.ts";
import { Direction } from "@/pdc/providers/superstruct/shared/index.ts";
import { Incentive } from "@/pdc/services/observatory/dto/Incentive.ts";
import { IncentiveRepositoryInterfaceResolver } from "../../interfaces/IncentiveRepositoryProviderInterface.ts";

export type ResultInterface = {
  code: Incentive["code"];
  libelle: string;
  direction: Infer<typeof Direction>;
  collectivite: number;
  operateur: number;
  autres: number;
}[];

@handler({
  service: "observatory",
  method: "getIncentive",
  middlewares: [[
    "validate",
    Incentive,
  ]],
  apiRoute: {
    path: "/observatory/incentive",
    method: "GET",
    public: true,
  },
})
export class IncentiveAction extends AbstractAction {
  constructor(private repository: IncentiveRepositoryInterfaceResolver) {
    super();
  }

  public async handle(params: Incentive): Promise<ResultInterface> {
    return this.repository.getIncentive(params);
  }
}
