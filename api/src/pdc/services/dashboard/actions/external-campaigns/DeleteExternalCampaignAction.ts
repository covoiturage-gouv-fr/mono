import { ContextType, handler, NotFoundException } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { copyGroupIdAndApplyGroupPermissionMiddlewares } from "@/pdc/providers/middleware/index.ts";
import { ExternalCampaignId } from "@/pdc/services/dashboard/dto/ExternalCampaigns.ts";
import { assertCanWrite, callerOf } from "@/pdc/services/dashboard/helpers/externalCampaignAccess.ts";
import { withCallerTerritory } from "@/pdc/services/dashboard/helpers/externalCampaignTerritory.ts";
import { ExternalCampaignsRepositoryInterfaceResolver } from "@/pdc/services/dashboard/interfaces/ExternalCampaignsRepositoryInterface.ts";
import { externalCampaignsFeatureMiddleware } from "@/pdc/services/dashboard/middlewares/ExternalCampaignsFeatureMiddleware.ts";

@handler({
  service: "dashboard",
  method: "deleteExternalCampaign",
  middlewares: [
    externalCampaignsFeatureMiddleware(),
    ["validate", ExternalCampaignId],
    ...copyGroupIdAndApplyGroupPermissionMiddlewares({
      registry: "registry.external_campaign.delete",
      operator: "operator.external_campaign.delete",
      territory: "territory.external_campaign.delete",
    }),
  ],
  apiRoute: {
    path: "/dashboard/external-campaign/:id",
    action: "dashboard:deleteExternalCampaign",
    method: "DELETE",
  },
})
export class DeleteExternalCampaignAction extends AbstractAction {
  constructor(private repository: ExternalCampaignsRepositoryInterfaceResolver) {
    super();
  }

  public override async handle(params: ExternalCampaignId, context: ContextType): Promise<{ success: boolean }> {
    const existing = await this.repository.find(params.id);
    if (!existing) throw new NotFoundException(`external campaign not found: (${params.id})`);
    const caller = await withCallerTerritory(this.repository, callerOf(context, "registry.external_campaign.delete"));
    assertCanWrite(existing, null, caller);
    await this.repository.delete(params.id);
    return { success: true };
  }
}
