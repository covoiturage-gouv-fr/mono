import { NotFoundException } from "@/ilos/common/exceptions/index.ts";
import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { logger } from "@/lib/logger/index.ts";
import { hasPermissionMiddleware } from "../../../providers/middleware/middlewares.ts";
import { handlerConfig, ParamsInterface, ResultInterface } from "../contracts/fail.contract.ts";
import { alias } from "../contracts/fail.schema.ts";
import { ExportStatus } from "../models/Export.ts";
import { ExportRepositoryInterfaceResolver } from "../repositories/ExportRepository.ts";
import { NotificationService } from "../services/NotificationService.ts";

@handler({
  ...handlerConfig,
  middlewares: [
    hasPermissionMiddleware("datalake.export.process"),
    ["validate", alias],
  ],
  apiRoute: { path: "/exports/:uuid/fail", method: "POST" },
})
export class FailAction extends AbstractAction {
  constructor(
    protected exportRepository: ExportRepositoryInterfaceResolver,
    protected notification: NotificationService,
  ) {
    super();
  }

  protected override async handle(params: ParamsInterface): Promise<ResultInterface> {
    const exp = await this.exportRepository.get(params.uuid);
    if (!exp) throw new NotFoundException(`Export ${params.uuid} not found`);

    // idempotent no-op: already failed, do not re-notify
    if (exp.status === ExportStatus.FAILURE) return { status: exp.status };
    // only a running export may fail — never downgrade a completed one
    if (exp.status !== ExportStatus.RUNNING) return { status: exp.status };

    // sets status = failure, stores the error JSON and logs the failure event
    await this.exportRepository.error(exp._id, params.message);

    // notifications best-effort: the row is already 'failure', a missing creator
    // (or mail outage) must not undo it — mirror CompleteAction's non-fatal email
    try {
      await this.notification.error({ ...exp, error: params.message });
      await this.notification.support({ ...exp, error: params.message });
    } catch (e) {
      logger.error(`Export ${exp.uuid} failed but notification failed: ${(e as Error).message}`);
    }

    return { status: ExportStatus.FAILURE };
  }
}
