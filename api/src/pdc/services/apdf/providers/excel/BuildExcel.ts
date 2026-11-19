import { defaultTimezone } from "@/config/time.ts";
import { KernelInterfaceResolver, provider } from "@/ilos/common/index.ts";
import { errorCode, taggedLogger } from "@/lib/logger/tagged.ts";
import { APDFNameProvider } from "@/pdc/providers/storage/index.ts";
import { ExcelCampaignConfig } from "@/pdc/services/apdf/interfaces/ExcelTypes.ts";
import excel from "dep:excel";
import { ResultInterface as Campaign } from "../../../policy/contracts/find.contract.ts";
import { SliceStatInterface } from "../../contracts/interfaces/PolicySliceStatInterface.ts";
import { CampaignSearchParamsInterface } from "../../interfaces/APDFRepositoryProviderInterface.ts";
import { DataRepositoryProvider } from "../DataRepositoryProvider.ts";
import { SlicesWorksheetWriter } from "./SlicesWorksheetWriter.ts";
import { TripsWorksheetWriter } from "./TripsWorksheetWriter.ts";
import { wrapSlices } from "./wrapSlicesHelper.ts";

const log = taggedLogger("[apdf]");

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

  // Sous Deno, une erreur d'ouverture du fichier (EACCES...) n'est pas relayée et commit() ne se termine jamais.
  public static async failOnStreamError(
    wbWriter: excel.stream.xlsx.WorkbookWriter,
    work: () => Promise<void>,
  ): Promise<void> {
    const stream = (wbWriter as unknown as { stream?: NodeJS.EventEmitter }).stream;
    let onError: ((err: Error) => void) | undefined;
    const failed = new Promise<never>((_, reject) => {
      onError = reject;
      stream?.once("error", onError);
    });
    try {
      await Promise.race([work(), failed]);
    } finally {
      if (onError) stream?.off("error", onError);
      failed.catch(() => {});
    }
  }

  async call(
    campaign: Campaign,
    start_date: Date,
    end_date: Date,
    operator_id: number,
  ): Promise<{ filename: string; filepath: string }> {
    // Delta déclaré vs calculé (GEN-643) : activé par la config de campagne
    // extras.declared_incentives.siren = SIREN du territoire financeur. Absent → export inchangé.
    const declared_siren = (campaign.params?.extras as { declared_incentives?: { siren?: string } })
      ?.declared_incentives?.siren ?? null;

    const params = {
      start_date,
      end_date,
      operator_id,
      campaign_id: campaign._id,
      declared_siren,
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
    await BuildExcel.failOnStreamError(wbWriter, async () => {
      if (this.hasSliceTrips(slices)) await this.writeSlices(wbWriter, slices, Boolean(declared_siren));
      await this.writeTrips(wbWriter, params);
      await wbWriter.commit();
    });

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
      log.error("write trips failed", { campaign_id: params.campaign_id, error: errorCode(e) });
    }
  }

  private async writeSlices(
    wkw: excel.stream.xlsx.WorkbookWriter,
    slices: SliceStatInterface[],
    withDeclared: boolean,
  ): Promise<void> {
    try {
      if (!slices.length) return;
      await this.slicesWsWriter.call(wkw, slices, withDeclared);
    } catch (e) {
      log.error("compute slices failed", { error: errorCode(e) });
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
      log.warn("campaign not found", { campaign_id });
      return defaultParams;
    }

    return {
      tz: campaign.params?.tz || defaultTimezone,
      booster_dates: new Set<string>([...(campaign.params?.booster_dates || [])]),
      extras: campaign.params?.extras || {},
    };
  }
}
