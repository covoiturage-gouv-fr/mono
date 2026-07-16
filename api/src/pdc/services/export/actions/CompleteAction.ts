import { NotFoundException } from "@/ilos/common/exceptions/index.ts";
import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { logger } from "@/lib/logger/index.ts";
import { hasPermissionMiddleware } from "../../../providers/middleware/middlewares.ts";
import { handlerConfig, ParamsInterface, ResultInterface } from "../contracts/complete.contract.ts";
import { alias } from "../contracts/complete.schema.ts";
import { ExportStatus } from "../models/Export.ts";
import { ExportRepositoryInterfaceResolver } from "../repositories/ExportRepository.ts";
import { NotificationService } from "../services/NotificationService.ts";

@handler({
  ...handlerConfig,
  middlewares: [
    hasPermissionMiddleware("datalake.export.process"),
    ["validate", alias],
  ],
  apiRoute: { path: "/exports/:uuid/complete", method: "POST" },
})
export class CompleteAction extends AbstractAction {
  constructor(
    protected exportRepository: ExportRepositoryInterfaceResolver,
    protected notification: NotificationService,
  ) {
    super();
  }

  protected override async handle(params: ParamsInterface): Promise<ResultInterface> {
    const exp = await this.exportRepository.get(params.uuid);
    if (!exp) throw new NotFoundException(`Export ${params.uuid} not found`);

    // idempotent no-op: already succeeded, do not email a second time
    if (exp.status === ExportStatus.SUCCESS) return { status: exp.status };
    // only a running export may complete — never resurrect one the reaper failed
    if (exp.status !== ExportStatus.RUNNING) return { status: exp.status };

    const filename = `${exp.uuid}.csv.zip`;
    await this.exportRepository.update(exp._id, { filename, file_size: params.file_size });
    await this.exportRepository.status(exp._id, ExportStatus.SUCCESS);

    // notify the export creator it is ready — best-effort: the file exists and the
    // row is SUCCESS, so a missing creator must not block/undo completion
    try {
      await this.notification.success(exp);
    } catch (e) {
      logger.error(`Export ${exp.uuid} completed but notification failed: ${(e as Error).message}`);
    }

    return { status: ExportStatus.SUCCESS };
  }
}
