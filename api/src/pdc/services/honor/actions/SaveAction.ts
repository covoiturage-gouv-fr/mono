import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { hasPermissionMiddleware } from "@/pdc/providers/middleware/index.ts";

import { handlerConfig, ParamsInterface, ResultInterface } from "../contracts/save.contract.ts";
import { alias } from "../contracts/save.schema.ts";
import { HonorRepositoryInterfaceResolver } from "../providers/HonorRepositoryProvider.ts";

@handler({
  ...handlerConfig,
  middlewares: [
    ["validate", alias],
    hasPermissionMiddleware("common.honor.save"),
  ],
})
export class SaveAction extends AbstractAction {
  constructor(private pg: HonorRepositoryInterfaceResolver) {
    super();
  }

  public override async handle(params: ParamsInterface): Promise<ResultInterface> {
    await this.pg.save(params.type, params.employer ?? "");
  }
}
