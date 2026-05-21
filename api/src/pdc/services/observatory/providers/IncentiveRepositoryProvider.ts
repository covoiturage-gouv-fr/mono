import { provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { join, raw } from "@/lib/pg/sql.ts";
import { getTableName } from "@/pdc/services/observatory/helpers/tableName.ts";
import {
  IncentiveParamsInterface,
  IncentiveRepositoryInterface,
  IncentiveRepositoryInterfaceResolver,
  IncentiveResultInterface,
} from "@/pdc/services/observatory/interfaces/IncentiveRepositoryProviderInterface.ts";

@provider({
  identifier: IncentiveRepositoryInterfaceResolver,
})
export class IncentiveRepositoryProvider implements IncentiveRepositoryInterface {
  private readonly table = (
    params: IncentiveParamsInterface,
  ) => {
    return getTableName(params, "observatoire_stats", "incentive");
  };
  constructor(private pgConnection: DenoPostgresConnection) {}

  async getIncentive(
    params: IncentiveParamsInterface,
  ): Promise<IncentiveResultInterface> {
    const tableName = this.table(params);
    const filters = [
      sql`year = ${params.year}`,
      sql`type = ${params.type}`,
      sql`code = ${params.code}`,
    ];
    if (params.direction) {
      filters.push(sql`direction = ${params.direction}`);
    }
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
        collectivite,
        operateur,
        autres 
      FROM ${raw(tableName)}
      WHERE ${join(filters, " AND ")}
    `;
    return await this.pgConnection.query<IncentiveResultInterface[number]>(query);
  }
}
