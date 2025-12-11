import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { contentBlacklistMiddleware, hasPermissionMiddleware } from "@/pdc/providers/middleware/index.ts";

import {
  handlerConfig,
  ParamsInterface,
  ResultInterface,
} from "@/pdc/services/territory/contracts/findGeoBySiren.contract.ts";
import { alias } from "@/pdc/services/territory/contracts/findGeoBySiren.schema.ts";
import { blacklist } from "../../config/filterOutput.ts";
import { GeoRepositoryProviderInterfaceResolver } from "../../interfaces/GeoRepositoryProviderInterface.ts";

@handler({
  ...handlerConfig,
  middlewares: [
    hasPermissionMiddleware("common.territory.list"),
    ["validate", alias],
    contentBlacklistMiddleware(...blacklist),
  ],
  apiRoute: {
    path: "/territory/findGeoBySiren",
    method: "POST",
  },
})
export class FindGeoBySirenAction extends AbstractAction {
  constructor(private geoRepository: GeoRepositoryProviderInterfaceResolver) {
    super();
  }

  public override async handle(params: ParamsInterface): Promise<ResultInterface> {
    return this.geoRepository.findBySiren(params);
  }
}
