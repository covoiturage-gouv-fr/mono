import { coerceDate } from "@/ilos/cli/index.ts";
import { command, CommandInterface, ConfigInterfaceResolver } from "@/ilos/common/index.ts";
import { logger } from "@/lib/logger/index.ts";
import { today, toTzString } from "@/pdc/helpers/dates.helper.ts";
import { DataGouvAPIProvider } from "@/pdc/providers/datagouv/DataGouvAPIProvider.ts";
import { DataGouvMetadataProvider } from "@/pdc/providers/datagouv/DataGouvMetadataProvider.ts";
import { Timezone } from "@/pdc/providers/validator/index.ts";
import { CSVWriter } from "@/pdc/services/export/models/CSVWriter.ts";
import { ExportTarget } from "@/pdc/services/export/models/Export.ts";
import { ExportParams } from "@/pdc/services/export/models/ExportParams.ts";
import { CarpoolRepository } from "@/pdc/services/export/repositories/CarpoolRepository.ts";
import { NotificationService } from "@/pdc/services/export/services/NotificationService.ts";
import { StorageService } from "@/pdc/services/export/services/StorageService.ts";
import { ExportRepositoryInterfaceResolver } from "../repositories/ExportRepository.ts";
import { DataGouvListType } from "../repositories/queries/datagouvListQuery.ts";
import { DataGouvStatsType } from "../repositories/queries/datagouvStatsQuery.ts";
import { DataGouvFileCreatorServiceInterfaceResolver } from "../services/DataGouvFileCreatorService.ts";
import { FieldServiceInterfaceResolver } from "../services/FieldService.ts";
import { LogServiceInterfaceResolver } from "../services/LogService.ts";
import { NameServiceInterfaceResolver } from "../services/NameService.ts";

export type Options = {
  start: Date;
  end: Date;
  tz: Timezone;
};

function defaultDate(offset = 0): Date {
  const now = today();
  return new Date(now.getFullYear(), now.getMonth() + offset, 1);
}

/**
 * Guard against publishing an empty dataset on data.gouv.fr.
 *
 * The export depends on trusted_zone.insee_counters (sqlmesh @monthly model
 * that withholds the current month until it is complete). If the export runs
 * before that month is materialised, the INNER JOIN yields zero rows and we
 * would otherwise upload an empty file. Abort loudly instead.
 */
export function assertDatagouvNotEmpty(stats: DataGouvStatsType, filename: string): void {
  if (Number(stats?.count_exposed) >= 1) return;

  throw new Error(
    `[export:datagouv] Refusing to upload empty dataset ${filename} ` +
      `(count_exposed=${stats?.count_exposed ?? 0}, count_total=${stats?.count_total ?? 0}). ` +
      `Upstream data likely not ready (trusted_zone.insee_counters @monthly).`,
  );
}

@command({
  signature: "export:datagouv",
  description: "Export file and upload to data.gouv.fr",
  options: [
    {
      signature: "-s, --start <start>",
      description: "Start date (YYYY-MM-DD)",
      coerce: coerceDate,
      default: defaultDate(-1),
    },
    {
      signature: "-e, --end <end>",
      description: "End date (YYYY-MM-DD)",
      coerce: coerceDate,
      default: defaultDate(0),
    },
    {
      signature: "--tz <tz>",
      description: "Output timezone",
      default: "Europe/Paris",
    },
  ],
})
export class DataGouvCommand implements CommandInterface {
  constructor(
    protected config: ConfigInterfaceResolver,
    protected api: DataGouvAPIProvider,
    protected metadata: DataGouvMetadataProvider,
    protected carpoolRepository: CarpoolRepository,
    protected exportRepository: ExportRepositoryInterfaceResolver,
    protected fileCreatorService: DataGouvFileCreatorServiceInterfaceResolver,
    protected fieldService: FieldServiceInterfaceResolver,
    protected nameService: NameServiceInterfaceResolver,
    protected logger: LogServiceInterfaceResolver,
    protected storage: StorageService,
    protected notify: NotificationService,
  ) {}

  public async call(options: Options): Promise<void> {
    if (!this.config.get("datagouv.api.enabled")) {
      logger.warn("DataGouv Export is DISABLED");
      return;
    }

    // 1. Get and configure parameters
    // 2. Export the file
    // 3. FileCreator
    // 4. Build data.gouv.fr Metadata
    // 5. Upload the resource to data.gouv.fr

    // TODO notify users of the end of the process

    const chunks = this.splitByMonth(options.start, options.end, options.tz);
    const target = ExportTarget.DATAGOUV;
    const fields = this.fieldService.byTarget<DataGouvListType>(target);

    for (const params of chunks) {
      const filename = this.nameService.datagouv(params.get().start_at);

      const s = toTzString(params.get().start_at, "Europe/Paris", "yyyy-MM");
      const e = toTzString(params.get().end_at, "Europe/Paris", "yyyy-MM");
      logger.info(`Exporting ${filename} from ${s}-01 to ${e}-01`);

      const path = await this.fileCreatorService.write(
        params,
        new CSVWriter<DataGouvListType>(filename, { tz: options.tz, compress: false, fields }),
      );

      // generate dataset description
      logger.info(`Generating metadata for ${filename}`);
      const stats = await this.carpoolRepository.datagouvStats(params);
      assertDatagouvNotEmpty(stats, filename);
      const description = this.metadata.description(stats);

      // upload to storage
      if (this.config.get("datagouv.api.upload")) {
        logger.info(`Uploading ${filename} to storage`);
        const dataset = await this.api.dataset();
        const resource = await this.api.upload(path);
        await this.api.setMetadata(resource, { description });
        logger.info(`Resource uploaded to ${dataset.page}`);
      } else {
        logger.warn("Datagouv upload is disabled");
      }
    }
  }

  private splitByMonth(start: Date, end: Date, tz: Timezone): ExportParams[] {
    const start_at = new Date(start.getFullYear(), start.getMonth(), 1);
    const end_at = new Date(end.getFullYear(), end.getMonth(), 1);
    const months: ExportParams[] = [];

    if (start_at >= end_at) {
      logger.error("Invalid date range. Defaults applied");

      return [ExportParams.fromJSON({
        start_at: defaultDate(-1),
        end_at: defaultDate(0),
        tz,
      })];
    }

    while (start_at < end_at) {
      const next = new Date(start_at);
      next.setMonth(next.getMonth() + 1);
      months.push(ExportParams.fromJSON({ start_at, end_at: next, tz }));
      start_at.setMonth(start_at.getMonth() + 1);
    }

    return months;
  }
}
