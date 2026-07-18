import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { copyGroupIdAndApplyGroupPermissionMiddlewares } from "@/pdc/providers/middleware/index.ts";
import { userScopeGuardMiddleware } from "@/pdc/services/dashboard/middlewares/UserScopeGuardMiddleware.ts";
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

  public override async handle(data: CreateUser): Promise<ResultInterface> {
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
