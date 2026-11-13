import { ContextType, handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { hasPermissionMiddleware } from "@/pdc/providers/middleware/middlewares.ts";
import { ExternalCampaignId } from "@/pdc/services/dashboard/dto/ExternalCampaigns.ts";
import { ExternalCampaignsRepositoryInterfaceResolver } from "@/pdc/services/dashboard/interfaces/ExternalCampaignsRepositoryInterface.ts";
import { externalCampaignsFeatureMiddleware } from "@/pdc/services/dashboard/middlewares/ExternalCampaignsFeatureMiddleware.ts";

@handler({
  service: "dashboard",
  method: "validateExternalCampaign",
  middlewares: [
    externalCampaignsFeatureMiddleware(),
    ["validate", ExternalCampaignId],
    hasPermissionMiddleware("registry.external_campaign.validate"),
  ],
  apiRoute: {
    path: "/dashboard/external-campaign/validate",
    action: "dashboard:validateExternalCampaign",
    method: "POST",
  },
})
export class ValidateExternalCampaignAction extends AbstractAction {
  constructor(private repository: ExternalCampaignsRepositoryInterfaceResolver) {
    super();
  }

  public override async handle(params: ExternalCampaignId, context: ContextType): Promise<{ success: boolean }> {
    await this.repository.validate(params.id, context?.call?.user?._id ?? null);
    return { success: true };
  }
}
