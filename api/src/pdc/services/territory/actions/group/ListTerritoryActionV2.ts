import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { copyFromContextMiddleware, hasPermissionMiddleware } from "@/pdc/providers/middleware/index.ts";

import { Infer, object, optional } from "@/lib/superstruct/index.ts";
import { Boolean, Id, Varchar } from "@/pdc/providers/superstruct/shared/index.ts";
import { ResultInterface } from "@/pdc/services/territory/contracts/list.contract.ts";
import { TerritoryRepositoryProviderInterfaceResolver } from "../../interfaces/TerritoryRepositoryProviderInterface.ts";

export const TerritoryParamsValidator = object({
  id: optional(Id),
  page: optional(Id),
  limit: optional(Id),
  offset: optional(Id),
  search: optional(Varchar),
  operator_id: optional(Id),
  policy: optional(Boolean),
});
export type TerritoryParams = Infer<typeof TerritoryParamsValidator>;

@handler({
  service: "territory",
  method: "territoryList",
  middlewares: [
    copyFromContextMiddleware(`call.user.operator_id`, "operator_id", false),
    hasPermissionMiddleware("common.territory.list"),
    ["validate-superstruct", TerritoryParamsValidator],
  ],
  apiRoute: {
    path: "/dashboard/territories",
    action: "dashboard:territories",
    successHttpCode: 200,
    method: "GET",
  },
})
export class ListTerritoryActionV2 extends AbstractAction {
  constructor(
    private territoryRepository: TerritoryRepositoryProviderInterfaceResolver,
  ) {
    super();
  }

  public override async handle(params: TerritoryParams): Promise<ResultInterface> {
    return this.territoryRepository.list(params);
  }
}
