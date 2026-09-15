import { ContextType, handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { copyGroupIdAndApplyGroupPermissionMiddlewares } from "@/pdc/providers/middleware/index.ts";
import { Users } from "@/pdc/services/dashboard/dto/Users.ts";
import { UsersRepositoryInterfaceResolver } from "@/pdc/services/dashboard/interfaces/UsersRepositoryInterface.ts";
import { MANAGE_SCOPES_PERMISSION } from "@/pdc/services/dashboard/middlewares/UserScopeGuardMiddleware.ts";

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

/**
 * Sans `manageScopes`, ni le détail des périmètres ni le SIREN de connexion : le premier
 * cartographie les rattachements hors du périmètre de l'appelant, le second sert au contrôle
 * ProConnect. Seul `scopes_count` reste, il porte les libellés de suppression du dashboard.
 */
export function maskPrivilegedFields(result: ResultInterface, context: ContextType): ResultInterface {
  const permissions = (context?.call?.user?.permissions ?? []) as string[];
  if (permissions.includes(MANAGE_SCOPES_PERMISSION)) return result;

  return {
    ...result,
    data: result.data.map(({ login_siren: _login_siren, scopes: _scopes, ...row }) => ({ ...row, scopes: [] })),
  };
}

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

  public override async handle(params: Users, context: ContextType): Promise<ResultInterface> {
    return maskPrivilegedFields(await this.repository.getUsers(params), context);
  }
}
