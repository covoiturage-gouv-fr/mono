import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { hasPermissionMiddleware } from "@/pdc/providers/middleware/index.ts";
import { DexClient } from "@/pdc/services/auth/providers/DexClient.ts";
import { handlerConfig, ParamsInterface, ResultInterface } from "../contracts/delete.contract.ts";
import { alias } from "../contracts/delete.schema.ts";
import { OperatorRepositoryProviderInterfaceResolver } from "../interfaces/OperatorRepositoryProviderInterface.ts";

@handler({
  ...handlerConfig,
  middlewares: [hasPermissionMiddleware("registry.operator.delete"), [
    "validate",
    alias,
  ]],
})
export class DeleteOperatorAction extends AbstractAction {
  constructor(private operatorRepository: OperatorRepositoryProviderInterfaceResolver, private dexClient: DexClient) {
    super();
  }

  public override async handle(params: ParamsInterface): Promise<ResultInterface> {
    // Soft delete keeps the row: the operator's API keys must go explicitly
    await this.dexClient.deleteAllByOperator(params._id);
    await this.operatorRepository.delete(params._id);
    return true;
  }
}
