import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { errorCode, taggedLogger } from "@/lib/logger/tagged.ts";
import { copyGroupIdAndApplyGroupPermissionMiddlewares } from "@/pdc/providers/middleware/helpers.ts";
import { DexClient } from "@/pdc/services/auth/providers/DexClient.ts";
import { DeleteOperator } from "@/pdc/services/dashboard/dto/Operators.ts";
import { OperatorsRepositoryInterfaceResolver } from "@/pdc/services/dashboard/interfaces/OperatorsRepositoryInterface.ts";

const log = taggedLogger("[operator]");

export type ResultInterface = {
  success: boolean;
  message: string;
};

@handler({
  service: "dashboard",
  method: "deleteOperator",
  middlewares: [
    ["validate", DeleteOperator],
    ...copyGroupIdAndApplyGroupPermissionMiddlewares({
      registry: "registry.operator.delete",
      operator: "operator.operator.delete",
    }),
  ],
  apiRoute: {
    path: "/dashboard/operator/:id",
    action: "dashboard:deleteOperator",
    method: "DELETE",
  },
})
export class DeleteOperatorAction extends AbstractAction {
  constructor(private repository: OperatorsRepositoryInterfaceResolver, private dexClient: DexClient) {
    super();
  }

  public override async handle(params: DeleteOperator): Promise<ResultInterface> {
    // Soft delete keeps the row: the operator's API keys must go explicitly
    try {
      await this.dexClient.deleteAllByOperator(params.id);
    } catch (e) {
      log.error("operator kept, keys not deleted", { operator_id: params.id, error: errorCode(e) });
      throw e;
    }
    const result = await this.repository.deleteOperator(params);
    log.info("operator deleted", { operator_id: params.id });
    return result;
  }
}
