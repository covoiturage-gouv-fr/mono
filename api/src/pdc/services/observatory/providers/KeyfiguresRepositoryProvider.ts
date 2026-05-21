import { provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { join, raw } from "@/lib/pg/sql.ts";
import { getTableName } from "@/pdc/services/observatory/helpers/tableName.ts";
import { checkTerritoryParam } from "../helpers/checkParams.ts";
import {
  KeyfiguresParamsInterface,
  KeyfiguresRepositoryInterface,
  KeyfiguresRepositoryInterfaceResolver,
  KeyfiguresResultInterface,
} from "../interfaces/KeyfiguresRepositoryProviderInterface.ts";

@provider({
  identifier: KeyfiguresRepositoryInterfaceResolver,
})
export class KeyfiguresRepositoryProvider implements KeyfiguresRepositoryInterface {
  private readonly table = (
    params: KeyfiguresParamsInterface,
    table: "flux" | "occupation" | "newcomer",
  ) => {
    return getTableName(params, "observatoire_stats", table);
  };
  constructor(private pgConnection: DenoPostgresConnection) {}

  // Retourne les données de la table observatory.monthly_flux pour le mois et l'année
  // et le type de territoire en paramètres
  // Paramètres optionnels observe et code pour filtrer les résultats sur un territoire
  async getKeyfigures(
    params: KeyfiguresParamsInterface,
  ): Promise<KeyfiguresResultInterface> {
    const typeParam = checkTerritoryParam(params.type);
    const joinOnB = [
      sql`(a.territory_1 = b.code OR a.territory_2 = b.code)`,
      sql`a.type = b.type`,
      sql`a.year = b.year`,
    ];
    const joinOnC = [
      sql`(a.territory_1 = c.code OR a.territory_2 = c.code)`,
      sql`a.type = c.type`,
      sql`a.year = c.year`,
    ];
    const filters = [
      sql`a.year = ${params.year}`,
      sql`a.type = ${typeParam}`,
      sql`b.code = ${params.code}`,
      sql`c.code = ${params.code}`,
    ];
    const intraJourneysFilters = [
      sql`territory_1 = territory_2`,
      sql`year = ${params.year}`,
      sql`type =  ${typeParam}`,
      sql`territory_1 = ${params.code}`,
    ];
    if (params.direction) {
      filters.push(sql`b.direction = ${params.direction}`);
      filters.push(sql`c.direction = ${params.direction}`);
    }
    if (params.month) {
      joinOnB.push(sql`a.month = b.month`);
      joinOnC.push(sql`a.month = c.month`);
      if (params.direction) {
        intraJourneysFilters.push(sql`month = ${params.month}`);
        filters.push(sql`a.month = ${params.month}`);
      }
    }
    if (params.trimester) {
      joinOnB.push(sql`a.trimester = b.trimester`);
      joinOnC.push(sql`a.trimester = c.trimester`);
      if (params.direction) {
        intraJourneysFilters.push(sql`trimester= ${params.trimester}`);
        filters.push(sql`a.trimester = ${params.trimester}`);
      }
    }
    if (params.semester) {
      joinOnB.push(sql`a.semester = b.semester`);
      joinOnC.push(sql`a.semester = c.semester`);
      if (params.direction) {
        intraJourneysFilters.push(sql`semester= ${params.semester}`);
        filters.push(sql`a.semester = ${params.semester}`);
      }
    }

    const intraJourneysQuery = sql`
      SELECT journeys 
      FROM ${raw(this.table(params, "flux"))}
      WHERE ${join(intraJourneysFilters, " AND ")}
    `;

    const query = sql`
      SELECT 
        b.code,b.libelle,b.direction,
        sum(a.passengers)::int AS passengers,
        sum(a.distance)::int AS distance,
        sum(a.duration)::int AS duration,
        b.journeys::int,
        (${intraJourneysQuery})::int as intra_journeys,
        b.occupation_rate::float,
        c.new_drivers::int,
        c.new_passengers::int
      FROM ${raw(this.table(params, "flux"))} a
      LEFT JOIN ${raw(this.table(params, "occupation"))} b ON ${join(joinOnB, " AND ")} 
      LEFT JOIN ${raw(this.table(params, "newcomer"))} c ON ${join(joinOnC, " AND ")}
      WHERE ${join(filters, " AND ")}
      GROUP BY b.code,b.libelle,b.direction,b.journeys,b.occupation_rate,c.new_drivers,c.new_passengers;
    `;
    return await this.pgConnection.query<KeyfiguresResultInterface[number]>(query);
  }
}
