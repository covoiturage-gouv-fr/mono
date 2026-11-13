import { ContextType, handler, NotFoundException } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { copyGroupIdAndApplyGroupPermissionMiddlewares } from "@/pdc/providers/middleware/index.ts";
import { UpdateExternalCampaign } from "@/pdc/services/dashboard/dto/ExternalCampaigns.ts";
import { assertCanWrite, callerOf } from "@/pdc/services/dashboard/helpers/externalCampaignAccess.ts";
import {
  resolveOwnTerritory,
  withCallerTerritory,
} from "@/pdc/services/dashboard/helpers/externalCampaignTerritory.ts";
import { ExternalCampaignsRepositoryInterfaceResolver } from "@/pdc/services/dashboard/interfaces/ExternalCampaignsRepositoryInterface.ts";
import { externalCampaignsFeatureMiddleware } from "@/pdc/services/dashboard/middlewares/ExternalCampaignsFeatureMiddleware.ts";

@handler({
  service: "dashboard",
  method: "updateExternalCampaign",
  middlewares: [
    externalCampaignsFeatureMiddleware(),
    ["validate", UpdateExternalCampaign],
    ...copyGroupIdAndApplyGroupPermissionMiddlewares({
      registry: "registry.external_campaign.update",
      operator: "operator.external_campaign.update",
      territory: "territory.external_campaign.update",
    }),
  ],
  apiRoute: {
    path: "/dashboard/external-campaign",
    action: "dashboard:updateExternalCampaign",
    method: "PUT",
  },
})
export class UpdateExternalCampaignAction extends AbstractAction {
  constructor(private repository: ExternalCampaignsRepositoryInterfaceResolver) {
    super();
  }

  public override async handle(data: UpdateExternalCampaign, context: ContextType): Promise<{ success: boolean }> {
    const existing = await this.repository.find(data.id);
    if (!existing) throw new NotFoundException(`external campaign not found: (${data.id})`);
    const caller = await withCallerTerritory(this.repository, callerOf(context, "registry.external_campaign.update"));
    assertCanWrite(existing, data.operator_ids, caller);
    // Un territoire ne peut pas déplacer une campagne : elle reste sur son territoire.
    const territory = !caller.registry && caller.territory_id !== null
      ? await resolveOwnTerritory(this.repository, caller.territory_id)
      : {};
    await this.repository.update({ ...data, ...territory });
    return { success: true };
  }
}
