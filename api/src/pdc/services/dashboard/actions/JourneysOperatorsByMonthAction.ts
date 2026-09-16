import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { copyFromContextMiddleware, hasPermissionMiddleware } from "@/pdc/providers/middleware/middlewares.ts";
import { JourneysByMonth } from "@/pdc/services/dashboard/dto/Journeys.ts";
import { CallerScope } from "@/pdc/services/dashboard/interfaces/JourneysRepositoryInterface.ts";
import { JourneysRepositoryInterfaceResolver } from "@/pdc/services/dashboard/interfaces/JourneysRepositoryInterface.ts";
export type ResultInterface = {
  year: number;
  month: number;
  campaign_id: string;
  operator_id: number;
  operator_name: string;
  journeys: number;
  incented_journeys: number;
  incentive_amount: number;
};

@handler({
  service: "dashboard",
  method: "journeysOperatorsByMonth",
  middlewares: [
    ["validate", JourneysByMonth],
    hasPermissionMiddleware("common.observatory.stats"),
    // Périmètre depuis la session (preserve=false) : le DTO refuse ces champs dans le corps,
    // et la statistique est cloisonnée en aval sur ces valeurs.
    copyFromContextMiddleware("call.user.territory_id", "territory_id", false),
    copyFromContextMiddleware("call.user.operator_id", "operator_id", false),
  ],
  apiRoute: {
    path: "/dashboard/operators/month",
    action: "dashboard:journeysOperatorsByMonth",
    method: "GET",
  },
})
export class JourneysOperatorsByMonthAction extends AbstractAction {
  constructor(private repository: JourneysRepositoryInterfaceResolver) {
    super();
  }

  public override async handle(params: JourneysByMonth & CallerScope): Promise<ResultInterface[]> {
    return this.repository.getOperatorsByMonth(params);
  }
}
