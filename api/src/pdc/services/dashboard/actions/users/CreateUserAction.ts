import { ContextType, handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { copyGroupIdAndApplyGroupPermissionMiddlewares } from "@/pdc/providers/middleware/index.ts";
import {
  MANAGE_SCOPES_PERMISSION,
  userScopeGuardMiddleware,
} from "@/pdc/services/dashboard/middlewares/UserScopeGuardMiddleware.ts";
import { CreateUser } from "@/pdc/services/dashboard/dto/Users.ts";
import { BrevoProviderInterfaceResolver } from "@/pdc/services/dashboard/interfaces/BrevoProviderInterface.ts";
import { OperatorsRepositoryInterfaceResolver } from "@/pdc/services/dashboard/interfaces/OperatorsRepositoryInterface.ts";
import { TerritoriesRepositoryInterfaceResolver } from "@/pdc/services/dashboard/interfaces/TerritoriesRepositoryInterface.ts";
import { UsersRepositoryInterfaceResolver } from "@/pdc/services/dashboard/interfaces/UsersRepositoryInterface.ts";

export type ResultInterface = {
  success: boolean;
  message: string;
};

@handler({
  service: "dashboard",
  method: "createUser",
  middlewares: [
    ["validate", CreateUser],
    ...copyGroupIdAndApplyGroupPermissionMiddlewares({
      registry: "registry.user.create",
      territory: "territory.user.create",
      operator: "operator.user.create",
    }),
    userScopeGuardMiddleware(),
  ],
  apiRoute: {
    path: "/dashboard/user",
    action: "dashboard:createUser",
    method: "POST",
  },
})
export class CreateUserAction extends AbstractAction {
  constructor(
    private repository: UsersRepositoryInterfaceResolver,
    private operatorsRepository: OperatorsRepositoryInterfaceResolver,
    private territoriesRepository: TerritoriesRepositoryInterfaceResolver,
    private brevoProvider: BrevoProviderInterfaceResolver,
  ) {
    super();
  }

  public override async handle(params: CreateUser, context: ContextType): Promise<ResultInterface> {
    const data = this.scopedPayload(params, context);
    const result = await this.repository.createUser(data);

    try {
      const siret = await this.getSiretForUser(data.operator_id, data.territory_id);
      await this.brevoProvider.sendWelcomeEmail({
        email: data.email,
        siret: siret || "",
      });
    } catch (_error) {
      // Email failure should not prevent user creation
    }

    return result;
  }

  /**
   * Le périmètre du compte créé vient de la **session**, pas du corps de la requête.
   *
   * `CreateUser` accepte `territory_id` / `operator_id`, et la recopie depuis le contexte
   * préserve la valeur fournie : sans cela, un administrateur de territoire pourrait créer
   * un compte rattaché à un territoire qui n'est pas le sien.
   */
  private scopedPayload(params: CreateUser, context: ContextType): CreateUser {
    const caller = context?.call?.user;
    const permissions = (caller?.permissions ?? []) as string[];
    if (permissions.includes(MANAGE_SCOPES_PERMISSION)) return params;

    if (caller?.operator_id != null) {
      return { ...params, operator_id: caller.operator_id, territory_id: null };
    }
    if (caller?.territory_id != null) {
      return { ...params, territory_id: caller.territory_id, operator_id: null };
    }
    // Ni périmètre ni permission d'administration : aucun rattachement possible (fail-closed).
    return { ...params, operator_id: null, territory_id: null };
  }

  private async getSiretForUser(
    operatorId?: number | null,
    territoryId?: number | null,
  ): Promise<string | null> {
    if (operatorId) {
      const operator = await this.operatorsRepository.getOperatorById(operatorId);
      return operator?.siret || null;
    }

    if (territoryId) {
      const territory = await this.territoriesRepository.getTerritoryById(territoryId);
      return territory?.siret || null;
    }

    return null;
  }
}
