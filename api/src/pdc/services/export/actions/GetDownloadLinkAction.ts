import { NotFoundException } from "../../../../ilos/common/exceptions/index.ts";
import { ContextType, handler } from "../../../../ilos/common/index.ts";
import { Action as AbstractAction } from "../../../../ilos/core/index.ts";
import { hasPermissionMiddleware } from "../../../providers/middleware/middlewares.ts";
import { handlerConfig, ParamsInterface, ResultInterface } from "../contracts/getDownloadLink.contract.ts";
import { alias } from "../contracts/getDownloadLink.schema.ts";
import { ExportRepositoryInterfaceResolver } from "../repositories/ExportRepository.ts";
import { StorageService } from "../services/StorageService.ts";

@handler({
  ...handlerConfig,
  middlewares: [
    hasPermissionMiddleware("common.export.list"),
    ["validate", alias],
  ],
  apiRoute: {
    path: "/exports/download-link/:id",
    method: "GET",
  },
})
export class GetDownloadLinkAction extends AbstractAction {
  constructor(
    protected storage: StorageService,
    protected exportRepository: ExportRepositoryInterfaceResolver,
  ) {
    super();
  }

  protected override async handle(
    params: ParamsInterface,
    context: ContextType,
  ): Promise<ResultInterface> {
    const userId = context.call?.user?._id;

    const exportEntity = await this.exportRepository.get(params.id, userId);
    if (!exportEntity) {
      throw new NotFoundException(`Export ${params.id} not found`);
    }
    const url = await this.storage.getOneTimeSignedUrl(exportEntity.filename);
    return {
      meta: null,
      data: {
        url,
      },
    };
  }
}
