import { ContextType, handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { copyGroupIdAndApplyGroupPermissionMiddlewares } from "@/pdc/providers/middleware/index.ts";
import { Users } from "@/pdc/services/dashboard/dto/Users.ts";
import { UsersRepositoryInterfaceResolver } from "@/pdc/services/dashboard/interfaces/UsersRepositoryInterface.ts";
import { maskPrivilegedFields, UserResult } from "@/pdc/services/dashboard/actions/users/UsersAction.ts";
export type ResultInterface = {
  meta: {
    page: number;
    total: number;
    totalPages: number;
  };
  data: UserResult[];
};

// `method` distinct de UsersAction : les deux déclaraient `dashboard:users`, et le registre
// des handlers garde silencieusement le premier enregistré — ce handler-ci ne servait jamais.
@handler({
  service: "dashboard",
  method: "user",
  middlewares: [
    ["validate", Users],
    ...copyGroupIdAndApplyGroupPermissionMiddlewares({
      registry: "registry.user.list",
      territory: "territory.user.list",
      operator: "operator.user.list",
    }),
  ],
  apiRoute: {
    path: "/dashboard/user/:id",
    action: "dashboard:user",
    method: "GET",
  },
})
export class UserAction extends AbstractAction {
  constructor(private repository: UsersRepositoryInterfaceResolver) {
    super();
  }

  public override async handle(params: Users, context: ContextType): Promise<ResultInterface> {
    return maskPrivilegedFields(await this.repository.getUsers(params), context);
  }
}
