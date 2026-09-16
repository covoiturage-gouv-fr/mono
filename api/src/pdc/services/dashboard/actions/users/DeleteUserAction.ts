import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { copyGroupIdAndApplyGroupPermissionMiddlewares } from "@/pdc/providers/middleware/index.ts";
import { SessionRepository } from "@/pdc/services/auth/providers/SessionRepository.ts";
import { DeleteUser } from "@/pdc/services/dashboard/dto/Users.ts";
import { UsersRepositoryInterfaceResolver } from "@/pdc/services/dashboard/interfaces/UsersRepositoryInterface.ts";
export type ResultInterface = {
  success: boolean;
  message: string;
  outcome: "user_deleted" | "scope_released";
};

@handler({
  service: "dashboard",
  method: "deleteUser",
  middlewares: [
    ["validate", DeleteUser],
    ...copyGroupIdAndApplyGroupPermissionMiddlewares({
      registry: "registry.user.delete",
      territory: "territory.user.delete",
      operator: "operator.user.delete",
    }),
  ],
  apiRoute: {
    path: "/dashboard/user/:id",
    action: "dashboard:deleteUser",
    method: "DELETE",
  },
})
export class DeleteUserAction extends AbstractAction {
  constructor(
    private repository: UsersRepositoryInterfaceResolver,
    private sessionRepository: SessionRepository,
  ) {
    super();
  }

  public override async handle(params: DeleteUser): Promise<ResultInterface> {
    const result = await this.repository.deleteUser(params);
    // Purge aussi sur retrait de périmètre : sinon la session en cours garde le territoire révoqué.
    await this.sessionRepository.destroyByUser(params.id);
    return result;
  }
}
