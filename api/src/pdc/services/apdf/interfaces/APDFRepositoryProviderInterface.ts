import { Cursor } from "@/ilos/connection-postgres/index.ts";
import { UnboundedSlices } from "../../policy/contracts/common/interfaces/Slices.ts";
import { PolicyStatsInterface } from "../contracts/interfaces/PolicySliceStatInterface.ts";
import { APDFTripInterface } from "./APDFTripInterface.ts";

export interface TzResultInterface {
  name: string;
  utc_offset: string;
}

export interface CampaignSearchParamsInterface {
  campaign_id: number;
  operator_id: number;
  start_date: Date;
  end_date: Date;
}

export interface DataRepositoryInterface {
  getPolicyActiveOperators(
    campaign_id: number,
    start_date: Date,
    end_date: Date,
  ): Promise<number[]>;
  getPolicyStats(
    params: CampaignSearchParamsInterface,
    slices: UnboundedSlices | [],
  ): Promise<PolicyStatsInterface>;
  getPolicyCursor(
    params: CampaignSearchParamsInterface,
  ): Promise<Cursor<APDFTripInterface>>;
}

export abstract class DataRepositoryProviderInterfaceResolver implements DataRepositoryInterface {
  public async getPolicyActiveOperators(
    campaign_id: number,
    start_date: Date,
    end_date: Date,
  ): Promise<number[]> {
    throw new Error("Not implemented");
  }

  public async getPolicyStats(
    params: CampaignSearchParamsInterface,
    slices: UnboundedSlices | [],
  ): Promise<PolicyStatsInterface> {
    throw new Error("Not implemented");
  }

  public async getPolicyCursor(
    params: CampaignSearchParamsInterface,
  ): Promise<Cursor<APDFTripInterface>> {
    throw new Error("Not implemented");
  }
}
