import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { copyGroupIdAndApplyGroupPermissionMiddlewares } from "@/pdc/providers/middleware/index.ts";
import { SessionRepository } from "@/pdc/services/auth/providers/SessionRepository.ts";
import { userScopeGuardMiddleware } from "@/pdc/services/dashboard/middlewares/UserScopeGuardMiddleware.ts";
import { UpdateUser } from "@/pdc/services/dashboard/dto/Users.ts";
import { UsersRepositoryInterfaceResolver } from "@/pdc/services/dashboard/interfaces/UsersRepositoryInterface.ts";
export type ResultInterface = {
  success: boolean;
  message: string;
};

@handler({
  service: "dashboard",
  method: "updateUser",
  middlewares: [
    ["validate", UpdateUser],
    ...copyGroupIdAndApplyGroupPermissionMiddlewares({
      registry: "registry.user.update",
      territory: "territory.user.update",
      operator: "operator.user.update",
    }),
    userScopeGuardMiddleware(),
  ],
  apiRoute: {
    path: "/dashboard/user",
    action: "dashboard:updateUser",
    method: "PUT",
  },
})
export class UpdateUserAction extends AbstractAction {
  constructor(
    private repository: UsersRepositoryInterfaceResolver,
    private sessionRepository: SessionRepository,
  ) {
    super();
  }

  public override async handle(data: UpdateUser): Promise<ResultInterface> {
    const result = await this.repository.updateUser(data);
    // Toute modification (rôle/périmètre) invalide les sessions : re-login avec des scopes frais.
    await this.sessionRepository.destroyByUser(data.id);
    return result;
  }
}
