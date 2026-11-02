import { ContextType, handler, NotFoundException } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { copyGroupIdAndApplyGroupPermissionMiddlewares } from "@/pdc/providers/middleware/index.ts";
import { UpdateCampaignCondition } from "@/pdc/services/dashboard/dto/CampaignConditions.ts";
import { assertCanWrite, callerOf } from "@/pdc/services/dashboard/helpers/campaignConditionAccess.ts";
import {
  resolveOwnTerritory,
  withCallerTerritory,
} from "@/pdc/services/dashboard/helpers/campaignConditionTerritory.ts";
import { CampaignConditionsRepositoryInterfaceResolver } from "@/pdc/services/dashboard/interfaces/CampaignConditionsRepositoryInterface.ts";

@handler({
  service: "dashboard",
  method: "updateCampaignCondition",
  middlewares: [
    ["validate", UpdateCampaignCondition],
    ...copyGroupIdAndApplyGroupPermissionMiddlewares({
      registry: "registry.campaign_condition.update",
      operator: "operator.campaign_condition.update",
      territory: "territory.campaign_condition.update",
    }),
  ],
  apiRoute: {
    path: "/dashboard/campaign-condition",
    action: "dashboard:updateCampaignCondition",
    method: "PUT",
  },
})
export class UpdateCampaignConditionAction extends AbstractAction {
  constructor(private repository: CampaignConditionsRepositoryInterfaceResolver) {
    super();
  }

  public override async handle(data: UpdateCampaignCondition, context: ContextType): Promise<{ success: boolean }> {
    const existing = await this.repository.find(data.id);
    if (!existing) throw new NotFoundException(`campaign condition not found: (${data.id})`);
    const caller = await withCallerTerritory(this.repository, callerOf(context, "registry.campaign_condition.update"));
    assertCanWrite(existing, data.operator_ids, caller);
    // Un territoire ne peut pas déplacer une campagne : elle reste sur son territoire.
    const territory = !caller.registry && caller.territory_id !== null
      ? await resolveOwnTerritory(this.repository, caller.territory_id)
      : {};
    await this.repository.update({ ...data, ...territory });
    return { success: true };
  }
}
