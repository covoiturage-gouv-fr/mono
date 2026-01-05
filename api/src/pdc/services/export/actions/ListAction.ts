import { ContextType, handler } from "../../../../ilos/common/index.ts";
import { Action as AbstractAction } from "../../../../ilos/core/index.ts";
import { hasPermissionMiddleware } from "../../../providers/middleware/middlewares.ts";
import { handlerConfig, ParamsInterface, ResultInterface } from "../contracts/list.contract.ts";
import { alias } from "../contracts/list.schema.ts";
import { ExportRepositoryInterfaceResolver } from "../repositories/ExportRepository.ts";
import { TerritoryServiceInterfaceResolver } from "../services/TerritoryService.ts";

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
    const userId = context.call?.user?._id;

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
        download_url: exp.download_url,
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
