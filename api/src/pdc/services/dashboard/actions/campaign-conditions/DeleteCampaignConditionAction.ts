import { ContextType, handler, NotFoundException } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { copyGroupIdAndApplyGroupPermissionMiddlewares } from "@/pdc/providers/middleware/index.ts";
import { CampaignConditionId } from "@/pdc/services/dashboard/dto/CampaignConditions.ts";
import { assertCanWrite, callerOf } from "@/pdc/services/dashboard/helpers/campaignConditionAccess.ts";
import { withCallerTerritory } from "@/pdc/services/dashboard/helpers/campaignConditionTerritory.ts";
import { CampaignConditionsRepositoryInterfaceResolver } from "@/pdc/services/dashboard/interfaces/CampaignConditionsRepositoryInterface.ts";

@handler({
  service: "dashboard",
  method: "deleteCampaignCondition",
  middlewares: [
    ["validate", CampaignConditionId],
    ...copyGroupIdAndApplyGroupPermissionMiddlewares({
      registry: "registry.campaign_condition.delete",
      operator: "operator.campaign_condition.delete",
      territory: "territory.campaign_condition.delete",
    }),
  ],
  apiRoute: {
    path: "/dashboard/campaign-condition/:id",
    action: "dashboard:deleteCampaignCondition",
    method: "DELETE",
  },
})
export class DeleteCampaignConditionAction extends AbstractAction {
  constructor(private repository: CampaignConditionsRepositoryInterfaceResolver) {
    super();
  }

  public override async handle(params: CampaignConditionId, context: ContextType): Promise<{ success: boolean }> {
    const existing = await this.repository.find(params.id);
    if (!existing) throw new NotFoundException(`campaign condition not found: (${params.id})`);
    const caller = await withCallerTerritory(this.repository, callerOf(context, "registry.campaign_condition.delete"));
    assertCanWrite(existing, null, caller);
    await this.repository.delete(params.id);
    return { success: true };
  }
}
