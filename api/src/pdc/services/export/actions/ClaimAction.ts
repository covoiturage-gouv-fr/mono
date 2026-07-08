import { ContextType, handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { hasPermissionMiddleware } from "../../../providers/middleware/middlewares.ts";
import { handlerConfig, ParamsInterface, ResultInterface } from "../contracts/claim.contract.ts";
import { alias } from "../contracts/claim.schema.ts";
import { ExportRepositoryInterfaceResolver } from "../repositories/ExportRepository.ts";

@handler({
  ...handlerConfig,
  middlewares: [
    hasPermissionMiddleware("datalake.export.process"),
    ["validate", alias],
  ],
  apiRoute: { path: "/exports/claim", method: "POST", successHttpCode: 200 },
})
export class ClaimAction extends AbstractAction {
  constructor(protected exportRepository: ExportRepositoryInterfaceResolver) {
    super();
  }

  protected override async handle(
    params: ParamsInterface,
    _context: ContextType,
  ): Promise<ResultInterface> {
    // Opportunistic reaper: fail exports stuck 'running' past staleDelay.
    // This replaces ProcessCommand as the reaper caller.
    await this.exportRepository.failStaleExports();

    const exp = await this.exportRepository.claim(params.targets);
    if (!exp) return null;
    return { uuid: exp.uuid, target: exp.target, params: exp.params.get() };
  }
}
