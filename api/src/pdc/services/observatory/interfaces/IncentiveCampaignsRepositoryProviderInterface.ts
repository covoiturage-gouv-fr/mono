import type { ResultInterface as CampaignsResultInterface } from "@/pdc/services/observatory/actions/incentiveCampaigns/CampaignsAction.ts";
import type { IncentiveCampaigns as CampaignsParamsInterface } from "@/pdc/services/observatory/dto/IncentiveCampaigns.ts";

export type { CampaignsParamsInterface, CampaignsResultInterface };

export interface IncentiveCampaignsRepositoryInterface {
  getCampaigns(
    params: CampaignsParamsInterface,
  ): Promise<CampaignsResultInterface[]>;
}

export abstract class IncentiveCampaignsRepositoryInterfaceResolver implements IncentiveCampaignsRepositoryInterface {
  async getCampaigns(
    params: CampaignsParamsInterface,
  ): Promise<CampaignsResultInterface[]> {
    throw new Error();
  }
}
