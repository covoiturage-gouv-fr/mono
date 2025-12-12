import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { hasPermissionMiddleware } from "../../../providers/middleware/index.ts";
import { handlerConfig, ParamsInterface, ResultInterface } from "../contracts/updateDescriptiveSheetUrl.contract.ts";
import { alias } from "../contracts/updateDescriptiveSheetUrl.schema.ts";
import { PolicyRepositoryProviderInterfaceResolver } from "../interfaces/index.ts";

@handler({
  ...handlerConfig,
  middlewares: [
    hasPermissionMiddleware("registry.policy.patch"),
    ["validate", alias],
  ],
  apiRoute: {
    path: "/policies/update/descriptive-sheet-url",
    method: "POST",
  },
})
export class UpdateDescriptiveSheetUrlAction extends AbstractAction {
  constructor(
    private repository: PolicyRepositoryProviderInterfaceResolver,
  ) {
    super();
  }

  public async handle(params: ParamsInterface): Promise<ResultInterface> {
    return await this.repository.updateDescriptiveSheetUrl(
      params._id,
      params.descriptive_sheet_url,
    );
  }
}
