import { ContextType, ForbiddenException, handler } from "../../../../ilos/common/index.ts";
import { Action as AbstractAction } from "../../../../ilos/core/index.ts";
import { hasPermissionMiddleware } from "../../../providers/middleware/middlewares.ts";
import { handlerConfig, ParamsInterface, ResultInterface } from "../contracts/list.contract.ts";
import { alias } from "../contracts/list.schema.ts";
import { ExportRepositoryInterfaceResolver } from "../repositories/ExportRepository.ts";
import { TerritoryServiceInterfaceResolver } from "../services/TerritoryService.ts";

/**
 * Propriétaire obligatoire : le filtre `created_by` du dépôt est conditionnel, un identifiant
 * absent renverrait donc **tous** les exports. Or un jeton Bearer (credentials opérateur) porte
 * un rôle et un opérateur, mais aucun identifiant utilisateur : on refuse au lieu de tout exposer.
 */
export function requireUserId(context: ContextType): number {
  const userId = context.call?.user?._id;
  if (typeof userId !== "number") {
    throw new ForbiddenException("Export listing requires a user session");
  }
  return userId;
}

@handler({
  ...handlerConfig,
  middlewares: [
    hasPermissionMiddleware("common.export.list"),
    ["validate", alias],
  ],
  apiRoute: {
    path: "/exports/list",
    method: "POST",
  },
})
export class listAction extends AbstractAction {
  constructor(
    protected exportRepository: ExportRepositoryInterfaceResolver,
    protected territoryService: TerritoryServiceInterfaceResolver,
  ) {
    super();
  }

  protected override async handle(
    params: ParamsInterface,
    context: ContextType,
  ): Promise<ResultInterface> {
    const userId = requireUserId(context);

    const exports = await this.exportRepository.list({
      created_by: userId,
      days: params.days,
    });

    const data = await Promise.all(
      exports.map(async (exp) => ({
        uuid: exp.uuid,
        start_date: exp.params.get().start_at,
        end_date: exp.params.get().end_at,
        geo_selector: await this.territoryService.getTerritoryNames(exp.params.get().geo_selector),
        filename: exp.filename,
        file_size: exp.file_size,
        status: exp.status,
      })),
    );

    return {
      meta: null,
      data,
    };
  }
}
