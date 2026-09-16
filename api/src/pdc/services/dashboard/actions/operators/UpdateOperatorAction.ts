import { ContextType, ForbiddenException, handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { copyGroupIdAndApplyGroupPermissionMiddlewares } from "@/pdc/providers/middleware/index.ts";
import { UpdateOperator } from "@/pdc/services/dashboard/dto/Operators.ts";
import { OperatorsRepositoryInterfaceResolver } from "@/pdc/services/dashboard/interfaces/OperatorsRepositoryInterface.ts";
export type ResultInterface = {
  success: boolean;
  message: string;
};

@handler({
  service: "dashboard",
  method: "updateOperator",
  middlewares: [
    ["validate", UpdateOperator],
    ...copyGroupIdAndApplyGroupPermissionMiddlewares({
      registry: "registry.operator.update",
      operator: "operator.operator.update",
    }),
  ],
  apiRoute: {
    path: "/dashboard/operator",
    action: "dashboard:updateOperator",
    method: "PUT",
  },
})
export class UpdateOperatorAction extends AbstractAction {
  constructor(private repository: OperatorsRepositoryInterfaceResolver) {
    super();
  }

  public override async handle(data: UpdateOperator, context: ContextType): Promise<ResultInterface> {
    this.assertOwnOperator(data.id, context);
    return this.repository.updateOperator(data);
  }

  /**
   * L'opérateur modifié doit être celui de l'appelant.
   *
   * Le contrôle de périmètre porte sur `operator_id`, que le DTO ne déclare pas : il est donc
   * recopié depuis la session et se compare à lui-même. L'identifiant réellement modifié, `id`,
   * n'était vérifié nulle part.
   */
  private assertOwnOperator(targetId: number, context: ContextType): void {
    const caller = context?.call?.user;
    const permissions = (caller?.permissions ?? []) as string[];
    if (permissions.includes("registry.operator.update")) return;

    if (caller?.operator_id !== targetId) {
      throw new ForbiddenException("Invalid permissions");
    }
  }
}
