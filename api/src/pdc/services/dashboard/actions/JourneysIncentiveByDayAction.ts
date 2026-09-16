import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { copyFromContextMiddleware, hasPermissionMiddleware } from "@/pdc/providers/middleware/middlewares.ts";
import { JourneysByDay } from "@/pdc/services/dashboard/dto/Journeys.ts";
import { CallerScope } from "@/pdc/services/dashboard/interfaces/JourneysRepositoryInterface.ts";
import { JourneysRepositoryInterfaceResolver } from "../interfaces/JourneysRepositoryInterface.ts";
export type ResultInterface = {
  date: Date;
  campaign_id: number;
  journeys: number;
  incented_journeys: number;
  incentive_amount: number;
};

@handler({
  service: "dashboard",
  method: "journeysIncentiveByDay",
  middlewares: [
    ["validate", JourneysByDay],
    hasPermissionMiddleware("common.observatory.stats"),
    // Périmètre depuis la session (preserve=false) : le DTO refuse ces champs dans le corps,
    // et la statistique est cloisonnée en aval sur ces valeurs.
    copyFromContextMiddleware("call.user.territory_id", "territory_id", false),
    copyFromContextMiddleware("call.user.operator_id", "operator_id", false),
  ],
  apiRoute: {
    path: "/dashboard/incentive/day",
    action: "dashboard:journeysIncentiveByDay",
    method: "GET",
  },
})
export class JourneysIncentiveByDayAction extends AbstractAction {
  constructor(private repository: JourneysRepositoryInterfaceResolver) {
    super();
  }

  public override async handle(params: JourneysByDay & CallerScope): Promise<ResultInterface[]> {
    return this.repository.getIncentiveByDay(params);
  }
}
