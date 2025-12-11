import { provider } from "@/ilos/common/Decorators.ts";
import { ConfigInterfaceResolver } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/DenoPostgresConnection.ts";
import { logger } from "@/lib/logger/index.ts";
import { CarpoolAcquisitionStatusEnum } from "@/pdc/providers/carpool/interfaces/common.ts";
import { CarpoolRow } from "@/pdc/services/export/models/CarpoolRow.ts";
import { CSVWriter } from "@/pdc/services/export/models/CSVWriter.ts";
import { ExportParams } from "@/pdc/services/export/models/ExportParams.ts";
import { ExportProgress } from "@/pdc/services/export/repositories/ExportRepository.ts";
import { carpoolCountQuery } from "@/pdc/services/export/repositories/queries/carpoolCountQuery.ts";
import { carpoolListQuery, CarpoolListType } from "@/pdc/services/export/repositories/queries/carpoolListQuery.ts";
import {
  datagouvStatsQuery,
  DataGouvStatsType,
} from "@/pdc/services/export/repositories/queries/datagouvStatsQuery.ts";
import { IncentiveStatusEnum } from "@/pdc/services/policy/interfaces/index.ts";
import { datagouvListQuery, DataGouvListType } from "./queries/datagouvListQuery.ts";

export abstract class CarpoolRepositoryInterfaceResolver {
  public async list(_params: ExportParams, _fileWriter: CSVWriter<CarpoolListType>): Promise<void> {
    throw new Error("Not implemented");
  }
  public async listCount(_params: ExportParams): Promise<number> {
    throw new Error("Not implemented");
  }
  public async dataGouvList(_params: ExportParams, _fileWriter: CSVWriter<DataGouvListType>): Promise<void> {
    throw new Error("Not implemented");
  }
  public async dataGouvStats(_params: ExportParams): Promise<DataGouvStatsType> {
    throw new Error("Not implemented");
  }
}

export type DataGouvQueryConfig = {
  min_occurrences: number;
  acquisition_status: CarpoolAcquisitionStatusEnum;
  incentive_status: IncentiveStatusEnum;
};

@provider({
  identifier: CarpoolRepositoryInterfaceResolver,
})
export class CarpoolRepository {
  public readonly table = "carpool_v2.carpools";
  private readonly batchSize = 1000;
  private readonly datagouvConfig: DataGouvQueryConfig = {
    min_occurrences: 6,
    acquisition_status: CarpoolAcquisitionStatusEnum.Processed,
    incentive_status: IncentiveStatusEnum.Validated,
  };

  constructor(
    protected pgConnection: DenoPostgresConnection,
    protected config: ConfigInterfaceResolver,
  ) {}

  /**
   * List carpools for the general exports
   */
  public async list(
    params: ExportParams,
    fileWriter: CSVWriter<CarpoolListType>,
    progress?: ExportProgress,
  ): Promise<void> {
    try {
      const total = progress ? await this.listCount(params) : 1; // total number of rows
      logger.info(`[export:CarpoolRepository] Exporting ${total} rows`);

      await using cursor = await this.pgConnection.cursor<CarpoolListType>(carpoolListQuery(params));
      let done = 0; // track the number of rows read
      for await (const rows of cursor.read(this.batchSize)) {
        for (const row of rows) {
          await fileWriter.append(new CarpoolRow<CarpoolListType>(row));
        }

        done += rows.length;
        if (progress && total) await progress(((done / total) * 100) | 0);
      }
    } catch (e) {
      // e instanceof AggregateError && console.error(e.errors);
      const message = e instanceof Error ? e.message : String(e);
      logger.error(`[export:CarpoolRepository] ${message}`);
      // e instanceof Error && logger.error(e.stack);
      throw e;
    }
  }

  /**
   * Count the number of carpools for the general exports
   */
  public async listCount(params: ExportParams): Promise<number> {
    const rows = await this.pgConnection.query<{ count: string }>(carpoolCountQuery(params));
    return Number(rows[0].count);
  }

  /**
   * List carpools with specific filtering for the open data requirements
   */
  public async datagouvList(params: ExportParams, fileWriter: CSVWriter<DataGouvListType>): Promise<void> {
    try {
      const query = datagouvListQuery(params, this.datagouvConfig);
      await using cursor = await this.pgConnection.cursor<DataGouvListType>(query);
      for await (const rows of cursor.read(this.batchSize)) {
        for (const row of rows) {
          await fileWriter.append(new CarpoolRow<DataGouvListType>(row));
        }
      }
    } catch (e) {
      if (e instanceof Error) {
        logger.error(`[dataGouvList] ${e.message}`);
      } else {
        logger.error(`[dataGouvList]`, e);
      }
      throw e;
    }
  }

  /**
   * Get the statistics for the DataGouv description
   */
  public async datagouvStats(params: ExportParams): Promise<DataGouvStatsType> {
    const query = datagouvStatsQuery(params, this.datagouvConfig);
    const rows = await this.pgConnection.query<DataGouvStatsType>(query);
    return rows[0];
  }
}
