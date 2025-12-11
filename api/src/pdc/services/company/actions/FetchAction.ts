import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { hasPermissionMiddleware } from "@/pdc/providers/middleware/index.ts";

import { CompanyDataSourceProviderInterfaceResolver } from "../interfaces/CompanyDataSourceProviderInterface.ts";
import { CompanyRepositoryProviderInterfaceResolver } from "../interfaces/CompanyRepositoryProviderInterface.ts";

import { handlerConfig, ParamsInterface, ResultInterface } from "../contracts/fetch.contract.ts";
import { alias } from "../contracts/fetch.schema.ts";

@handler({
  ...handlerConfig,
  middlewares: [hasPermissionMiddleware("common.company.fetch"), [
    "validate",
    alias,
  ]],
  apiRoute: {
    path: "/company/fetch",
    method: "POST",
  },
})
export class FetchAction extends AbstractAction {
  constructor(
    private ds: CompanyDataSourceProviderInterfaceResolver,
    private repository: CompanyRepositoryProviderInterfaceResolver,
  ) {
    super();
  }

  public override async handle(params: ParamsInterface): Promise<ResultInterface> {
    const data = await this.ds.find(params);
    await this.repository.updateOrCreate(data);

    return this.repository.findBySiret(params);
  }
}
