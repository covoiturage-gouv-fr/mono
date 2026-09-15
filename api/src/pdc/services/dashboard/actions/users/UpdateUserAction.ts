import { ContextType, handler, NotFoundException } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { copyGroupIdAndApplyGroupPermissionMiddlewares } from "@/pdc/providers/middleware/index.ts";
import { SessionRepository } from "@/pdc/services/auth/providers/SessionRepository.ts";
import { UserScopeRepository } from "@/pdc/services/auth/providers/UserScopeRepository.ts";
import { UpdateUser } from "@/pdc/services/dashboard/dto/Users.ts";
import { UsersRepositoryInterfaceResolver } from "@/pdc/services/dashboard/interfaces/UsersRepositoryInterface.ts";
import {
  MANAGE_SCOPES_PERMISSION,
  userScopeGuardMiddleware,
} from "@/pdc/services/dashboard/middlewares/UserScopeGuardMiddleware.ts";
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
    private userScopeRepository: UserScopeRepository,
  ) {
    super();
  }

  public override async handle(params: UpdateUser, context: ContextType): Promise<ResultInterface> {
    await this.assertTargetInScope(params.id, context);

    const result = await this.repository.updateUser(this.sanitizePayload(params, context));
    // Toute modification (rôle/périmètre) invalide les sessions : re-login avec des scopes frais.
    await this.sessionRepository.destroyByUser(params.id);
    return result;
  }

  /**
   * Sans la permission d'administration, l'appelant ne modifie que l'identité et le rôle.
   *
   * Les champs de périmètre viennent du corps de la requête et `seedScopes` remplace tout le
   * pivot : les laisser passer permettrait de rattacher un compte à un autre opérateur, ou de
   * l'évincer de ses autres territoires. `login_siren` est déjà refusé par la garde, on le
   * retire aussi pour ne pas l'écraser par mégarde.
   */
  private sanitizePayload(params: UpdateUser, context: ContextType): UpdateUser {
    const permissions = (context?.call?.user?.permissions ?? []) as string[];
    if (permissions.includes(MANAGE_SCOPES_PERMISSION)) return params;

    const {
      operator_id: _operator_id,
      territory_id: _territory_id,
      login_siren: _login_siren,
      scopes: _scopes,
      ...rest
    } = params;
    return rest;
  }

  /**
   * Le compte modifié doit relever du périmètre de l'appelant.
   *
   * Le périmètre est lu dans la **session**, jamais dans les paramètres : `UpdateUser` accepte
   * `territory_id` / `operator_id`, et la recopie depuis le contexte préserve la valeur fournie
   * par l'appelant — s'y fier reviendrait à laisser l'attaquant choisir son propre contrôle.
   * Sans périmètre ni permission d'administration, on refuse (fail-closed).
   */
  private async assertTargetInScope(targetId: number, context: ContextType): Promise<void> {
    const caller = context?.call?.user;
    const permissions = (caller?.permissions ?? []) as string[];
    if (permissions.includes(MANAGE_SCOPES_PERMISSION)) return;

    const granted = caller?.operator_id != null
      ? await this.userScopeRepository.userHasOperator(targetId, caller.operator_id)
      : caller?.territory_id != null
      ? await this.userScopeRepository.userHasTerritory(targetId, caller.territory_id)
      : false;

    // Même réponse qu'un identifiant inexistant : ne pas révéler l'existence du compte.
    if (!granted) throw new NotFoundException();
  }
}
