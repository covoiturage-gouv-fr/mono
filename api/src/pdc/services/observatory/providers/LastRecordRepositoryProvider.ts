import { provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { join } from "@/lib/pg/sql.ts";
import { checkTerritoryParam } from "@/pdc/services/observatory/helpers/checkParams.ts";
import {
  LastRecordParamsInterface,
  LastRecordRepositoryInterface,
  LastRecordRepositoryInterfaceResolver,
  LastRecordResultInterface,
} from "@/pdc/services/observatory/interfaces/LastRecordRepositoryProviderInterface.ts";

@provider({
  identifier: LastRecordRepositoryInterfaceResolver,
})
export class LastRecordRepositoryProvider implements LastRecordRepositoryInterface {
  constructor(private pgConnection: DenoPostgresConnection) {}

  async getLastRecord(
    params: LastRecordParamsInterface,
    maxYear?: number,
    maxMonth?: number,
  ): Promise<LastRecordResultInterface> {
    const typeParam = checkTerritoryParam(params.type);
    const filters = [
      sql`type = ${typeParam}`,
      sql`(territory_1 = ${params.code} OR territory_2 = ${params.code})`,
    ];

    if (maxYear !== undefined && maxMonth !== undefined) {
      filters.push(
        sql`(year * 100 + month) <= ${maxYear * 100 + maxMonth}`,
      );
    }

    const query = sql`
      SELECT year, month
      FROM observatoire_stats.flux_by_month
      WHERE ${join(filters, " AND ")}
      ORDER BY year DESC, month DESC
      LIMIT 1
    `;

    const rows = await this.pgConnection.query<{ year: number; month: number }>(query);
    return rows.length > 0 ? { year: rows[0].year, month: rows[0].month } : null;
  }
}
