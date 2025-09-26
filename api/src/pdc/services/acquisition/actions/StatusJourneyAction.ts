import { ContextType, handler, NotFoundException } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { CarpoolStatusService } from "@/pdc/providers/carpool/providers/CarpoolStatusService.ts";
import { copyGroupIdAndApplyGroupPermissionMiddlewares } from "@/pdc/providers/middleware/index.ts";
import { handlerConfig, ParamsInterface, ResultInterface } from "../contracts/status.contract.ts";
import { alias } from "../contracts/status.schema.ts";

@handler({
  ...handlerConfig,
  middlewares: [
    ["validate", alias],
    ...copyGroupIdAndApplyGroupPermissionMiddlewares({
      operator: "operator.acquisition.status",
    }),
  ],
  apiRoute: {
    path: "/journeys/:operator_journey_id",
    method: "GET",
    rateLimiter: {
      key: "rl-acquisition-check",
      limit: 2_000,
      windowMinute: 1,
    },
    rpcAnswerOnSuccess: false,
    rpcAnswerOnFailure: true,
  },
})
export class StatusJourneyAction extends AbstractAction {
  constructor(
    private statusService: CarpoolStatusService,
  ) {
    super();
  }

  protected override async handle(params: ParamsInterface, context: ContextType): Promise<ResultInterface> {
    const { operator_journey_id, operator_id } = params;
    const result = await this.statusService.findByOperatorJourneyId(
      operator_id,
      operator_journey_id,
      context.call?.api_version_range || "3.1",
    );

    if (!result) {
      throw new NotFoundException();
    }

    return this.statusService.castToStatusResult(operator_journey_id, result);
  }
}
