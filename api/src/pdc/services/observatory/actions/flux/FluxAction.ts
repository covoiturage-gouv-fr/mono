import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { Flux } from "@/pdc/services/observatory/dto/flux/Flux.ts";
import { FluxRepositoryInterfaceResolver } from "../../interfaces/FluxRepositoryProviderInterface.ts";
export type ResultInterface = {
  ter_1: Flux["code"];
  lng_1: number;
  lat_1: number;
  ter_2: Flux["code"];
  lng_2: number;
  lat_2: number;
  passengers: number;
  distance: number;
  duration: number;
}[];

@handler({
  service: "observatory",
  method: "getFlux",
  middlewares: [[
    "validate",
    Flux,
  ]],
  apiRoute: {
    path: "/observatory/flux",
    method: "GET",
    public: true,
  },
})
export class FluxAction extends AbstractAction {
  constructor(private repository: FluxRepositoryInterfaceResolver) {
    super();
  }

  public async handle(params: Flux): Promise<ResultInterface> {
    return this.repository.getFlux(params);
  }
}
