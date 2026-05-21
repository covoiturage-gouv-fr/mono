import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { hasPermissionMiddleware } from "@/pdc/providers/middleware/index.ts";

import { handlerConfig, ParamsInterface, ResultInterface } from "../contracts/stats.contract.ts";
import { alias } from "../contracts/stats.schema.ts";
import { HonorRepositoryInterfaceResolver } from "../providers/HonorRepositoryProvider.ts";

@handler({
  ...handlerConfig,
  middlewares: [
    ["validate", alias],
    hasPermissionMiddleware("common.honor.stats"),
  ],
})
export class StatsAction extends AbstractAction {
  constructor(private pg: HonorRepositoryInterfaceResolver) {
    super();
  }

  public override async handle(params: ParamsInterface): Promise<ResultInterface> {
    return this.pg.stats(params);
  }
}
