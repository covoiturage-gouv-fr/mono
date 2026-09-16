import { handler } from "@/ilos/common/index.ts";
import { Action as AbstractAction } from "@/ilos/core/index.ts";
import { copyGroupIdAndApplyGroupPermissionMiddlewares } from "@/pdc/providers/middleware/index.ts";
import { CampaignApdf } from "@/pdc/services/dashboard/dto/CampaignApdf.ts";
import {
  CampaignsRepositoryInterfaceResolver,
  ScopedCampaignApdfParams,
} from "@/pdc/services/dashboard/interfaces/CampaignsRepositoryInterface.ts";

export type ResultInterface = {
  signed_url: string;
  key: string;
  size: number;
  operator_id: number;
  campaign_id: number;
  datetime: Date;
  name: string;
}[];

@handler({
  service: "dashboard",
  method: "campaignApdf",
  middlewares: [
    ["validate", CampaignApdf],
    ...copyGroupIdAndApplyGroupPermissionMiddlewares({
      territory: "territory.apdf.list",
      operator: "operator.apdf.list",
      registry: "registry.apdf.list",
    }),
  ],
  apiRoute: {
    path: "/dashboard/campaign-apdf",
    action: "dashboard:campaignApdf",
    method: "GET",
  },
})
export class CampaignApdfAction extends AbstractAction {
  constructor(private repository: CampaignsRepositoryInterfaceResolver) {
    super();
  }

  // `validate` précède la recopie de périmètre : l'appelant ne peut pas fournir lui-même
  // territory_id / operator_id (le DTO les refuse), ils viennent donc bien de sa session.
  public override async handle(params: ScopedCampaignApdfParams): Promise<ResultInterface> {
    return this.repository.getCampaignApdf(params);
  }
}
