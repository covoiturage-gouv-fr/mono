import { provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { join, raw } from "@/lib/pg/sql.ts";
import { getTableName } from "@/pdc/services/observatory/helpers/tableName.ts";
import {
  DistributionRepositoryInterface,
  DistributionRepositoryInterfaceResolver,
  JourneysByDistancesParamsInterface,
  JourneysByDistancesResultInterface,
  JourneysByHoursParamsInterface,
  JourneysByHoursResultInterface,
} from "@/pdc/services/observatory/interfaces/DistributionRepositoryProviderInterface.ts";

@provider({
  identifier: DistributionRepositoryInterfaceResolver,
})
export class DistributionRepositoryProvider implements DistributionRepositoryInterface {
  private readonly table = (
    params:
      | JourneysByHoursParamsInterface
      | JourneysByDistancesParamsInterface,
  ) => {
    return getTableName(params, "observatoire_stats", "distribution");
  };

  constructor(private pgConnection: DenoPostgresConnection) {}

  async getJourneysByHours(
    params: JourneysByHoursParamsInterface,
  ): Promise<JourneysByHoursResultInterface> {
    const tableName = this.table(params);
    const filters = [
      sql`year = ${params.year}`,
      sql`type = ${params.type}`,
      sql`code = ${params.code}`,
    ];
    if (params.month) {
      filters.push(sql`month = ${params.month}`);
    }
    if (params.trimester) {
      filters.push(sql`trimester = ${params.trimester}`);
    }
    if (params.semester) {
      filters.push(sql`semester = ${params.semester}`);
    }
    const query = sql`
      SELECT
        code,
        libelle,
        direction,
        hours
      FROM ${raw(tableName)}
      WHERE ${join(filters, " AND ")}
    `;
    return await this.pgConnection.query<JourneysByHoursResultInterface[number]>(query);
  }

  async getJourneysByDistances(
    params: JourneysByDistancesParamsInterface,
  ): Promise<JourneysByDistancesResultInterface> {
    const tableName = this.table(params);
    const filters = [
      sql`year = ${params.year}`,
      sql`type = ${params.type}`,
      sql`code = ${params.code}`,
      sql`direction = ${params.direction}`,
    ];
    if (params.month) {
      filters.push(sql`month = ${params.month}`);
    }
    if (params.trimester) {
      filters.push(sql`trimester = ${params.trimester}`);
    }
    if (params.semester) {
      filters.push(sql`semester = ${params.semester}`);
    }
    const query = sql`
      SELECT
        code,
        libelle,
        direction,
        distances
      FROM ${raw(tableName)}
      WHERE ${join(filters, " AND ")}
    `;
    return await this.pgConnection.query<JourneysByDistancesResultInterface[number]>(query);
  }
}
