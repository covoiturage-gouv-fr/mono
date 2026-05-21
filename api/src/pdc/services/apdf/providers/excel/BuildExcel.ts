import { defaultTimezone } from "@/config/time.ts";
import { KernelInterfaceResolver, provider } from "@/ilos/common/index.ts";
import { logger } from "@/lib/logger/index.ts";
import { APDFNameProvider } from "@/pdc/providers/storage/index.ts";
import { APDFTripInterface } from "@/pdc/services/apdf/interfaces/APDFTripInterface.ts";
import { ExcelCampaignConfig } from "@/pdc/services/apdf/interfaces/ExcelTypes.ts";
import excel from "dep:excel";
import { ResultInterface as Campaign } from "../../../policy/contracts/find.contract.ts";
import { SliceStatInterface } from "../../contracts/interfaces/PolicySliceStatInterface.ts";
import { CampaignSearchParamsInterface } from "../../interfaces/APDFRepositoryProviderInterface.ts";
import { DataRepositoryProvider } from "../DataRepositoryProvider.ts";
import { SlicesWorksheetWriter } from "./SlicesWorksheetWriter.ts";
import { TripsWorksheetWriter } from "./TripsWorksheetWriter.ts";
import { wrapSlices } from "./wrapSlicesHelper.ts";

@provider()
export class BuildExcel {
  constructor(
    private kernel: KernelInterfaceResolver,
    private apdfRepoProvider: DataRepositoryProvider,
    private TripsWsWriter: TripsWorksheetWriter,
    private slicesWsWriter: SlicesWorksheetWriter,
    private apdfNameProvider: APDFNameProvider,
  ) {}

  public static initWorkbookWriter(
    filepath: string,
  ): excel.stream.xlsx.WorkbookWriter {
    return new excel.stream.xlsx.WorkbookWriter({
      filename: filepath,
      useStyles: true,
    });
  }

  async call(
    campaign: Campaign,
    start_date: Date,
    end_date: Date,
    operator_id: number,
  ): Promise<{ filename: string; filepath: string }> {
    const params = {
      start_date,
      end_date,
      operator_id,
      campaign_id: campaign._id,
    };

    // fetch aggregated and slice data
    const {
      total_count: trips,
      total_sum: amount,
      subsidized_count: subsidized,
      slices,
    } = await this.apdfRepoProvider.getPolicyStats(params, wrapSlices(campaign.params.slices));

    // generate the filename and filepath
    const fileParams = {
      name: campaign.name,
      campaign_id: campaign._id,
      operator_id,
      datetime: start_date,
      trips,
      subsidized,
      amount,
    };
    const filename: string = this.apdfNameProvider.filename(fileParams);
    const filepath: string = this.apdfNameProvider.filepath(filename);

    // create the Workbook and add Worksheets
    const wbWriter: excel.stream.xlsx.WorkbookWriter = BuildExcel.initWorkbookWriter(filepath);
    if (this.hasSliceTrips(slices)) await this.writeSlices(wbWriter, slices);
    await this.writeTrips(wbWriter, params);
    await wbWriter.commit();

    return { filename, filepath };
  }

  private async writeTrips(
    wkw: excel.stream.xlsx.WorkbookWriter,
    params: CampaignSearchParamsInterface,
  ): Promise<void> {
    try {
      const config = await this.getConfig(params.campaign_id);
      await using cursor = await this.apdfRepoProvider.getPolicyCursor(params);
      await this.TripsWsWriter.call(cursor, config, wkw);
    } catch (e) {
      logger.error(`[apdf:buildExcel] Error while writing trips. Campaign: ${params.campaign_id}`);
      if (e instanceof Error) {
        logger.error(e.message);
        logger.error(e.stack);
      }
    }
  }

  private async writeSlices(wkw: excel.stream.xlsx.WorkbookWriter, slices: SliceStatInterface[]): Promise<void> {
    try {
      if (!slices.length) return;
      await this.slicesWsWriter.call(wkw, slices);
    } catch (e) {
      logger.error("[apdf:buildExcel] Error while computing slices");
      if (e instanceof Error) {
        logger.error(e.message);
      }
    }
  }

  private hasSliceTrips(slices: SliceStatInterface[]): boolean {
    return slices.reduce((acc, s) => acc + s.count, 0) > 0;
  }

  private async getConfig(campaign_id: number): Promise<ExcelCampaignConfig> {
    const defaultParams = {
      tz: defaultTimezone,
      booster_dates: new Set<string>(),
      extras: {},
    };

    const campaign = await this.kernel.call(
      "campaign:find",
      { _id: campaign_id },
      {
        channel: { service: "apdf" },
        call: { user: { permissions: ["registry.policy.find"] } },
      },
    );

    if (!campaign) {
      logger.warn(`[BuildExcel] Campaign ${campaign_id} not found`);
      return defaultParams;
    }

    return {
      tz: campaign.params?.tz || defaultTimezone,
      booster_dates: new Set<string>([...(campaign.params?.booster_dates || [])]),
      extras: campaign.params?.extras || {},
    };
  }
}
