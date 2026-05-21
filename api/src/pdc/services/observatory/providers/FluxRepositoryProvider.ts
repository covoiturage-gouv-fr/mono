import { provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { empty, join, raw } from "@/lib/pg/sql.ts";
import { checkIndicParam, checkTerritoryParam } from "@/pdc/services/observatory/helpers/checkParams.ts";
import { getTableName } from "@/pdc/services/observatory/helpers/tableName.ts";
import {
  BestFluxParamsInterface,
  BestFluxResultInterface,
  EvolFluxParamsInterface,
  EvolFluxResultInterface,
  FluxParamsInterface,
  FluxRepositoryInterface,
  FluxRepositoryInterfaceResolver,
  FluxResultInterface,
} from "@/pdc/services/observatory/interfaces/FluxRepositoryProviderInterface.ts";

@provider({
  identifier: FluxRepositoryInterfaceResolver,
})
export class FluxRepositoryProvider implements FluxRepositoryInterface {
  private readonly perim_table = "geo.perimeters";
  private readonly table = (
    params:
      | FluxParamsInterface
      | EvolFluxParamsInterface
      | BestFluxParamsInterface,
  ) => {
    return getTableName(params, "observatoire_stats", "flux");
  };
  constructor(private pgConnection: DenoPostgresConnection) {}

  async getFlux(
    params: FluxParamsInterface,
  ): Promise<FluxResultInterface> {
    const tableName = this.table(params);
    const observeParam = checkTerritoryParam(params.observe);
    const typeParam = checkTerritoryParam(params.type);

    const perimTableQuery = sql`
      SELECT ${raw(observeParam)} 
      FROM (
        SELECT arr as com, epci, aom, dep, reg, country 
        FROM ${raw(this.perim_table)} 
        WHERE year = geo.get_latest_millesime_or(${params.year}::smallint)
      ) t 
      WHERE ${raw(typeParam)} = ${params.code}
    `;

    const filters = [
      sql`type = ${observeParam}`,
      sql`(distance / journeys) <= 80`,
      sql`(territory_1 IN (${perimTableQuery}) OR territory_2 IN (${perimTableQuery}))`,
      sql`territory_1 <> territory_2`,
      sql`year = ${params.year}`,
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
        l_territory_1 AS ter_1, lng_1, lat_1,
        l_territory_2 AS ter_2, lng_2, lat_2,
        passengers, distance, duration
      FROM ${raw(tableName)}
      WHERE ${join(filters, " AND ")}
    `;
    return await this.pgConnection.query<FluxResultInterface[number]>(query);
  }

  async getEvolFlux(
    params: EvolFluxParamsInterface,
  ): Promise<EvolFluxResultInterface> {
    const tableName = this.table(params);
    const indics = [
      "journeys",
      "passengers",
      "has_incentive",
      "distance",
      "duration",
    ];
    const indicParam = checkIndicParam(params.indic, indics, "journeys");
    const typeParam = sql`${checkTerritoryParam(params.type)}`;
    const limit = sql`${params.past ? Number(params.past) * 12 + 1 : 25}`;
    const selectedVar = [
      sql`year`,
      sql`sum(${raw(indicParam)}::numeric) AS ${raw(indicParam)}`,
    ];
    const filters = [
      sql`type = ${typeParam}`,
      sql`(territory_1 = ${params.code} OR territory_2 = ${params.code})`,
    ];
    const groupBy = [
      sql`year`,
    ];
    if (params.month) {
      selectedVar.push(sql`month`);
      groupBy.push(sql`month`);
    }
    if (params.trimester) {
      selectedVar.push(sql`trimester`);
      groupBy.push(sql`trimester`);
    }
    if (params.semester) {
      selectedVar.push(sql`semester`);
      groupBy.push(sql`semester`);
    }
    const query = sql`
      SELECT ${join(selectedVar, ", ")} 
      ${indicParam == `distance` ? sql`, sum(journeys) AS journeys` : empty}
      FROM ${raw(tableName)}
      WHERE ${join(filters, " AND ")}
      GROUP BY ${join(groupBy, ", ")} 
      ORDER BY (${join(groupBy, ", ")}) DESC
      LIMIT ${limit};
    `;
    return await this.pgConnection.query<EvolFluxResultInterface[number]>(query);
  }

  // Retourne les données pour le top 10 des trajets dans le dashboard
  async getBestFlux(
    params: BestFluxParamsInterface,
  ): Promise<BestFluxResultInterface> {
    const tableName = this.table(params);
    const typeParam = checkTerritoryParam(params.type);
    const perimTableQuery = sql`
      SELECT com
      FROM (
        SELECT  arr as com, epci, aom, dep, reg, country 
        FROM ${raw(this.perim_table)} 
        WHERE year = geo.get_latest_millesime_or(${params.year}::smallint)
      ) t 
      WHERE ${raw(typeParam)} = ${params.code}
    `;
    const filters = [
      sql`year = ${params.year}`,
      sql`(territory_1 IN (${perimTableQuery}) OR territory_2 IN (${perimTableQuery}))`,
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
      SELECT distinct territory_1, l_territory_1, territory_2, l_territory_2, journeys
      FROM ${raw(tableName)}
      WHERE ${join(filters, " AND ")}
      ORDER BY journeys DESC
      LIMIT ${params.limit}
    `;
    return await this.pgConnection.query<BestFluxResultInterface[number]>(query);
  }
}
