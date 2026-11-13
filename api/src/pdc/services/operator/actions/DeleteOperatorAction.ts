import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { errorCode, taggedLogger } from "@/lib/logger/tagged.ts";
import { hasPermissionMiddleware } from "@/pdc/providers/middleware/index.ts";
import { DexClient } from "@/pdc/services/auth/providers/DexClient.ts";
import { handlerConfig, ParamsInterface, ResultInterface } from "../contracts/delete.contract.ts";
import { alias } from "../contracts/delete.schema.ts";
import { OperatorRepositoryProviderInterfaceResolver } from "../interfaces/OperatorRepositoryProviderInterface.ts";

const log = taggedLogger("[operator]");

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
    try {
      await this.dexClient.deleteAllByOperator(params._id);
    } catch (e) {
      log.error("operator kept, keys not deleted", { operator_id: params._id, error: errorCode(e) });
      throw e;
    }
    await this.operatorRepository.delete(params._id);
    log.info("operator deleted", { operator_id: params._id });
    return true;
  }
}
