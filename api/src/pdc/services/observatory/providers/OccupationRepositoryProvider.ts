import { provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { join, raw } from "@/lib/pg/sql.ts";
import { checkIndicParam, checkTerritoryParam } from "@/pdc/services/observatory/helpers/checkParams.ts";
import { getTableName } from "@/pdc/services/observatory/helpers/tableName.ts";
import {
  BestTerritoriesParamsInterface,
  BestTerritoriesResultInterface,
  EvolOccupationParamsInterface,
  EvolOccupationResultInterface,
  OccupationParamsInterface,
  OccupationRepositoryInterface,
  OccupationRepositoryInterfaceResolver,
  OccupationResultInterface,
} from "@/pdc/services/observatory/interfaces/OccupationRepositoryProviderInterface.ts";

@provider({
  identifier: OccupationRepositoryInterfaceResolver,
})
export class OccupationRepositoryProvider implements OccupationRepositoryInterface {
  private readonly table = (
    params:
      | OccupationParamsInterface
      | EvolOccupationParamsInterface
      | BestTerritoriesParamsInterface,
  ) => {
    return getTableName(params, "observatoire_stats", "occupation");
  };
  private readonly perim_table = "geo.perimeters";

  constructor(private pgConnection: DenoPostgresConnection) {}

  async getOccupation(
    params: OccupationParamsInterface,
  ): Promise<OccupationResultInterface> {
    const tableName = this.table(params);
    const observeParam = checkTerritoryParam(params.observe);
    const typeParam = checkTerritoryParam(params.type);
    const selectedVar = [
      sql`year`,
      sql`type`,
      sql`code`,
      sql`libelle`,
      sql`journeys`,
      sql`occupation_rate`,
      sql`geom`,
    ];
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
      sql`year = ${params.year}`,
      sql`type = ${observeParam}`,
      sql`code IN (${perimTableQuery})`,
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
      ${join(selectedVar, ", ")}
      FROM ${raw(tableName)}
      WHERE ${join(filters, " AND ")}
    `;
    return await this.pgConnection.query<OccupationResultInterface[number]>(query);
  }

  // Retourne les données pour les graphiques construits à partir de la table observatory._occupation
  async getEvolOccupation(
    params: EvolOccupationParamsInterface,
  ): Promise<EvolOccupationResultInterface> {
    const tableName = this.table(params);
    const indics = [
      "journeys",
      "trips",
      "has_incentive",
      "occupation_rate",
    ];
    const indicParam = checkIndicParam(params.indic, indics, "journeys");
    const typeParam = checkTerritoryParam(params.type);
    const limit = params.past ? Number(params.past) * 12 + 1 : 25;
    const selectedVar = [
      sql`year`,
      sql`${raw(indicParam)}::float AS ${raw(indicParam)}`,
    ];
    const filters = [
      sql`type = ${typeParam}`,
      sql`code = ${params.code}`,
      sql`direction = 'both'`,
    ];
    const orderBy = [
      sql`year`,
    ];
    if (params.month) {
      selectedVar.push(sql`month`);
      orderBy.push(sql`month`);
    }
    if (params.trimester) {
      selectedVar.push(sql`trimester`);
      orderBy.push(sql`trimester`);
    }
    if (params.semester) {
      selectedVar.push(sql`semester`);
      orderBy.push(sql`semester`);
    }
    const query = sql`
      SELECT ${join(selectedVar, ", ")}
      FROM ${raw(tableName)}
      WHERE ${join(filters, " AND ")}
      ORDER BY (${join(orderBy, ", ")}) DESC
      LIMIT ${limit};
    `;

    return await this.pgConnection.query<EvolOccupationResultInterface[number]>(query);
  }

  // Retourne les données pour le top 10 des territoires dans le dashboard
  async getBestTerritories(
    params: BestTerritoriesParamsInterface,
  ): Promise<BestTerritoriesResultInterface> {
    const tableName = this.table(params);
    const typeParam = checkTerritoryParam(params.type);
    const observeParam = checkTerritoryParam(params.observe);
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
      sql`year = ${params.year}`,
      sql`type = ${observeParam}`,
      sql`direction = 'both'`,
      sql`code IN (${perimTableQuery})`,
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
      SELECT code, libelle, journeys
      FROM ${raw(tableName)}
      WHERE ${join(filters, " AND ")}
      ORDER BY journeys DESC
      LIMIT ${params.limit};
    `;

    return await this.pgConnection.query<BestTerritoriesResultInterface[number]>(query);
  }
}
