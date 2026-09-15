import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { copyGroupIdAndApplyGroupPermissionMiddlewares } from "@/pdc/providers/middleware/index.ts";
import { Users } from "@/pdc/services/dashboard/dto/Users.ts";
import { UsersRepositoryInterfaceResolver } from "@/pdc/services/dashboard/interfaces/UsersRepositoryInterface.ts";

export type UserResult = {
  id: number;
  firstname?: string;
  lastname?: string;
  email: string;
  operator_id?: number;
  territory_id?: number;
  phone?: string;
  role: string;
  login_siren?: string | null;
  // Nombre total de périmètres du compte, indépendant du filtre du caller.
  scopes_count: number;
  // Périmètres territoire du compte, défaut en tête.
  scopes: Array<{ territory_id: number; is_default: boolean }>;
};
export type ResultInterface = {
  meta: {
    page: number;
    total: number;
    totalPages: number;
  };
  data: UserResult[];
};

@handler({
  service: "dashboard",
  method: "users",
  middlewares: [
    ["validate", Users],
    ...copyGroupIdAndApplyGroupPermissionMiddlewares({
      registry: "registry.user.list",
      territory: "territory.user.list",
      operator: "operator.user.list",
    }),
  ],
  apiRoute: {
    path: "/dashboard/users",
    action: "dashboard:users",
    method: "GET",
  },
})
export class UsersAction extends AbstractAction {
  constructor(private repository: UsersRepositoryInterfaceResolver) {
    super();
  }

  public override async handle(params: Users): Promise<ResultInterface> {
    return this.repository.getUsers(params);
  }
}
