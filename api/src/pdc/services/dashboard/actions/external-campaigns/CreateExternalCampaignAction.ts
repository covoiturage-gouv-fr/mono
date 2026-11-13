import { ContextType, handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { copyGroupIdAndApplyGroupPermissionMiddlewares } from "@/pdc/providers/middleware/index.ts";
import { CreateExternalCampaign } from "@/pdc/services/dashboard/dto/ExternalCampaigns.ts";
import { assertCanCreate, callerOf } from "@/pdc/services/dashboard/helpers/externalCampaignAccess.ts";
import {
  resolveOwnTerritory,
  resolveSimulatedTerritory,
} from "@/pdc/services/dashboard/helpers/externalCampaignTerritory.ts";
import { ExternalCampaignsRepositoryInterfaceResolver } from "@/pdc/services/dashboard/interfaces/ExternalCampaignsRepositoryInterface.ts";
import { externalCampaignsFeatureMiddleware } from "@/pdc/services/dashboard/middlewares/ExternalCampaignsFeatureMiddleware.ts";

@handler({
  service: "dashboard",
  method: "createExternalCampaign",
  middlewares: [
    externalCampaignsFeatureMiddleware(),
    ["validate", CreateExternalCampaign],
    ...copyGroupIdAndApplyGroupPermissionMiddlewares({
      registry: "registry.external_campaign.create",
      operator: "operator.external_campaign.create",
      territory: "territory.external_campaign.create",
    }),
  ],
  apiRoute: {
    path: "/dashboard/external-campaign",
    action: "dashboard:createExternalCampaign",
    method: "POST",
  },
})
export class CreateExternalCampaignAction extends AbstractAction {
  constructor(private repository: ExternalCampaignsRepositoryInterfaceResolver) {
    super();
  }

  public override async handle(data: CreateExternalCampaign, context: ContextType): Promise<{ id: number }> {
    const caller = callerOf(context, "registry.external_campaign.create");
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
