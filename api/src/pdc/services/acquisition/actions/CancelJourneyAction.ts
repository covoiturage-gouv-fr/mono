import { ContextType, handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { copyGroupIdAndApplyGroupPermissionMiddlewares } from "@/pdc/providers/middleware/index.ts";

import { CarpoolAcquisitionService } from "@/pdc/providers/carpool/index.ts";
import { handlerConfig, ParamsInterface, ResultInterface } from "../contracts/cancel.contract.ts";
import { alias } from "../contracts/cancel.schema.ts";

@handler({
  ...handlerConfig,
  middlewares: [
    ["validate", alias],
    ...copyGroupIdAndApplyGroupPermissionMiddlewares({
      operator: "operator.acquisition.cancel",
    }),
  ],
  apiRoute: {
    path: "/journeys/:operator_journey_id/cancel",
    method: "POST",
    rateLimiter: {
      key: "rl-acquisition",
      limit: 10_000,
      windowMinute: 1,
    },
    rpcAnswerOnSuccess: true,
    rpcAnswerOnFailure: true,
  },
})
export class CancelJourneyAction extends AbstractAction {
  constructor(
    private acquisitionService: CarpoolAcquisitionService,
  ) {
    super();
  }

  protected override async handle(params: ParamsInterface, context: ContextType): Promise<ResultInterface> {
    const { operator_id, operator_journey_id } = params;
    await this.acquisitionService.cancelRequest({
      api_version: context.call?.api_version_range || "3",
      operator_id,
      operator_journey_id,
      cancel_code: params.code,
      cancel_message: params.message,
    });
  }
}
