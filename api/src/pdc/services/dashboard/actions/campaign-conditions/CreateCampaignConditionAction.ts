import { ContextType, handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { copyGroupIdAndApplyGroupPermissionMiddlewares } from "@/pdc/providers/middleware/index.ts";
import { CreateCampaignCondition } from "@/pdc/services/dashboard/dto/CampaignConditions.ts";
import { assertCanCreate, callerOf } from "@/pdc/services/dashboard/helpers/campaignConditionAccess.ts";
import {
  resolveOwnTerritory,
  resolveSimulatedTerritory,
} from "@/pdc/services/dashboard/helpers/campaignConditionTerritory.ts";
import { CampaignConditionsRepositoryInterfaceResolver } from "@/pdc/services/dashboard/interfaces/CampaignConditionsRepositoryInterface.ts";

@handler({
  service: "dashboard",
  method: "createCampaignCondition",
  middlewares: [
    ["validate", CreateCampaignCondition],
    ...copyGroupIdAndApplyGroupPermissionMiddlewares({
      registry: "registry.campaign_condition.create",
      operator: "operator.campaign_condition.create",
      territory: "territory.campaign_condition.create",
    }),
  ],
  apiRoute: {
    path: "/dashboard/campaign-condition",
    action: "dashboard:createCampaignCondition",
    method: "POST",
  },
})
export class CreateCampaignConditionAction extends AbstractAction {
  constructor(private repository: CampaignConditionsRepositoryInterfaceResolver) {
    super();
  }

  public override async handle(data: CreateCampaignCondition, context: ContextType): Promise<{ id: number }> {
    const caller = callerOf(context, "registry.campaign_condition.create");
    assertCanCreate(data.operator_ids, caller);
    if (caller.registry) {
      const territory = await resolveSimulatedTerritory(this.repository, caller, data.territory_id);
      return this.repository.create({ ...data, ...territory }, { operator_id: null, territory_id: null });
    }
    if (caller.territory_id !== null) {
      const territory = await resolveOwnTerritory(this.repository, caller.territory_id);
      return this.repository.create({ ...data, ...territory }, {
        operator_id: null,
        territory_id: caller.territory_id,
      });
    }
    return this.repository.create(data, { operator_id: caller.operator_id, territory_id: null });
  }
}
