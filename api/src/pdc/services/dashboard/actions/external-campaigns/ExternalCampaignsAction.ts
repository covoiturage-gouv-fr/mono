import { ContextType, ForbiddenException, handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { copyGroupIdAndApplyGroupPermissionMiddlewares } from "@/pdc/providers/middleware/index.ts";
import { ExternalCampaigns } from "@/pdc/services/dashboard/dto/ExternalCampaigns.ts";
import { Caller, callerOf, viewedAs } from "@/pdc/services/dashboard/helpers/externalCampaignAccess.ts";
import { withCallerTerritory } from "@/pdc/services/dashboard/helpers/externalCampaignTerritory.ts";
import {
  ExternalCampaignRow,
  ExternalCampaignsRepositoryInterfaceResolver,
} from "@/pdc/services/dashboard/interfaces/ExternalCampaignsRepositoryInterface.ts";
import { externalCampaignsFeatureMiddleware } from "@/pdc/services/dashboard/middlewares/ExternalCampaignsFeatureMiddleware.ts";

@handler({
  service: "dashboard",
  method: "externalCampaigns",
  middlewares: [
    externalCampaignsFeatureMiddleware(),
    ["validate", ExternalCampaigns],
    ...copyGroupIdAndApplyGroupPermissionMiddlewares({
      registry: "registry.external_campaign.list",
      operator: "operator.external_campaign.list",
      territory: "territory.external_campaign.list",
    }),
  ],
  apiRoute: {
    path: "/dashboard/external-campaigns",
    action: "dashboard:externalCampaigns",
    method: "GET",
  },
})
export class ExternalCampaignsAction extends AbstractAction {
  constructor(private repository: ExternalCampaignsRepositoryInterfaceResolver) {
    super();
  }

  // Un opérateur voit les campagnes où il est listé ; un territoire, celles qu'il a saisies ou qui le concernent.
  public override async handle(params: ExternalCampaigns, context: ContextType): Promise<ExternalCampaignRow[]> {
    return this.rowsFor(
      params,
      await withCallerTerritory(
        this.repository,
        viewedAs(callerOf(context, "registry.external_campaign.list"), params),
      ),
    );
  }

  private async rowsFor(params: ExternalCampaigns, caller: Caller): Promise<ExternalCampaignRow[]> {
    if (caller.registry) return this.repository.list({ id: params.id });
    if (caller.territory_id !== null) {
      return this.repository.list({
        id: params.id,
        territory: { id: caller.territory_id, resolved: caller.territory },
      });
    }
    if (caller.operator_id === null) throw new ForbiddenException("Invalid permissions");
    return this.repository.list({ id: params.id, operator_id: caller.operator_id });
  }
}
