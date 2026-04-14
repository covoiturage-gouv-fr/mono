import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { Location } from "@/pdc/services/observatory/dto/Location.ts";
import { LocationRepositoryInterfaceResolver } from "../../interfaces/LocationRepositoryProviderInterface.ts";
export type ResultInterface = {
  hex: string;
  count: number;
}[];

@handler({
  service: "observatory",
  method: "getLocation",
  middlewares: [[
    "validate",
    Location,
  ]],
  apiRoute: {
    path: "/observatory/location",
    method: "GET",
    public: true,
  },
})
export class LocationAction extends AbstractAction {
  constructor(private repository: LocationRepositoryInterfaceResolver) {
    super();
  }

  public async handle(params: Location): Promise<ResultInterface> {
    return this.repository.getLocation(params);
  }
}
