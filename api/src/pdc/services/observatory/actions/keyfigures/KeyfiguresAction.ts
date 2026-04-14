import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { Infer } from "@/lib/superstruct/index.ts";
import { Direction } from "@/pdc/providers/superstruct/shared/index.ts";
import { KeyFigures } from "@/pdc/services/observatory/dto/KeyFigures.ts";
import { KeyfiguresRepositoryInterfaceResolver } from "../../interfaces/KeyfiguresRepositoryProviderInterface.ts";
export type ResultInterface = {
  territory: KeyFigures["code"];
  l_territory: string;
  passengers: number;
  distance: number;
  duration: number;
  journeys: number;
  intra_journeys: number;
  has_incentive: number;
  occupation_rate: number;
  new_drivers: number;
  new_passengers: number;
  direction: Infer<typeof Direction>;
}[];

@handler({
  service: "observatory",
  method: "getKeyfigures",
  middlewares: [[
    "validate",
    KeyFigures,
  ]],
  apiRoute: {
    path: "/observatory/keyfigures",
    method: "GET",
    public: true,
  },
})
export class KeyfiguresAction extends AbstractAction {
  constructor(private repository: KeyfiguresRepositoryInterfaceResolver) {
    super();
  }

  public async handle(params: KeyFigures): Promise<ResultInterface> {
    return this.repository.getKeyfigures(params);
  }
}
