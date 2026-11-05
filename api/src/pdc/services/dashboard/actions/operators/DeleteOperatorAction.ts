import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { copyGroupIdAndApplyGroupPermissionMiddlewares } from "@/pdc/providers/middleware/helpers.ts";
import { DexClient } from "@/pdc/services/auth/providers/DexClient.ts";
import { DeleteOperator } from "@/pdc/services/dashboard/dto/Operators.ts";
import { OperatorsRepositoryInterfaceResolver } from "@/pdc/services/dashboard/interfaces/OperatorsRepositoryInterface.ts";
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
    await this.dexClient.deleteAllByOperator(params.id);
    return this.repository.deleteOperator(params);
  }
}
