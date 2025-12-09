import { provider } from "@/ilos/common/index.ts";
import { LegacyPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { join, raw } from "@/lib/pg/sql.ts";
import { latLngToCell } from "dep:h3-js";
import { checkTerritoryParam } from "../helpers/checkParams.ts";
import {
  LocationParamsInterface,
  LocationRepositoryInterface,
  LocationRepositoryInterfaceResolver,
  LocationResultInterface,
} from "../interfaces/LocationRepositoryProviderInterface.ts";

@provider({
  identifier: LocationRepositoryInterfaceResolver,
})
export class LocationRepositoryProvider implements LocationRepositoryInterface {
  private readonly table = "observatoire_stats.view_location";
  private readonly perim_table = "geo.perimeters";

  constructor(private pg: LegacyPostgresConnection) {}

  async getLocation(
    params: LocationParamsInterface,
  ): Promise<LocationResultInterface> {
    const result: LocationResultInterface = [];
    const typeParam = checkTerritoryParam(params.type);
    const perimTableQuery = sql`
      SELECT com 
      FROM (
        SELECT arr as com, epci, aom, dep, reg, country 
        FROM ${raw(this.perim_table)} 
        WHERE year = geo.get_latest_millesime_or(${params.year}::smallint)
      ) t 
      WHERE ${raw(typeParam)} = ${params.code}
    `;

    const filters = [
      sql`extract('year' from start_datetime) = ${params.year}`,
      sql`(start_geo_code IN (${perimTableQuery}) OR end_geo_code IN (${perimTableQuery}))`,
    ];

    if (params.month) {
      filters.push(sql`extract('month' from start_datetime) = ${params.month}`);
    }
    if (params.trimester) {
      filters.push(sql`extract('quarter' FROM start_datetime) = ${params.trimester}`);
    }
    if (params.semester) {
      filters.push(
        sql`(CASE WHEN extract('quarter' FROM start_datetime)::int > 3 THEN 2 ELSE 1 END) = ${params.semester}`,
      );
    }

    const query = sql`
      SELECT 
        start_lat as lat, 
        start_lon as lon
      FROM ${raw(this.table)}
      WHERE ${join(filters, " AND ")}
      UNION ALL
      SELECT
        end_lat as lat, 
        end_lon as lon
      FROM ${raw(this.table)} 
      WHERE ${join(filters, " AND ")}
    `;
    const response = await this.pg.getClient().query(query);
    const geomToHex = response.rows
      .map((r) => latLngToCell(r.lat, r.lon, params.zoom))
      .reduce<Record<string, number>>(
        (acc, curr) => ((acc[curr] = (acc[curr] || 0) + 1), acc),
        {},
      );
    Object.entries(geomToHex).forEach(([key, val]) => result.push({ hex: key, count: Number(val) }));
    return result;
  }
}
