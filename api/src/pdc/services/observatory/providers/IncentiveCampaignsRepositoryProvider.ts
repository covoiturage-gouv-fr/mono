import { provider } from "@/ilos/common/index.ts";
import { LegacyPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { join, raw } from "@/lib/pg/sql.ts";
import { checkTerritoryParam } from "../helpers/checkParams.ts";
import {
  CampaignsParamsInterface,
  CampaignsResultInterface,
  IncentiveCampaignsRepositoryInterface,
  IncentiveCampaignsRepositoryInterfaceResolver,
} from "../interfaces/IncentiveCampaignsRepositoryProviderInterface.ts";

@provider({
  identifier: IncentiveCampaignsRepositoryInterfaceResolver,
})
export class IncentiveCampaignsRepositoryProvider implements IncentiveCampaignsRepositoryInterface {
  private readonly table = "observatoire_stats.incentive_campaigns";
  private readonly perim_table = "geo_stats.perimeters_aggregate";

  constructor(private pg: LegacyPostgresConnection) {}

  async getCampaigns(
    params: CampaignsParamsInterface,
  ): Promise<CampaignsResultInterface> {
    const typeParam = params.type !== undefined ? checkTerritoryParam(params.type) : null;
    const filters = [
      sql`b.geom IS NOT NULL`,
    ];
    if (params.code) {
      filters.push(sql`left(a.code,9) = ${params.code}`);
    }
    if (params.year && !params.code) {
      filters.push(sql`EXTRACT(YEAR FROM a.date_fin) = ${params.year}::int`);
      filters.push(sql`a.date_fin < now()`);
    }
    if (params.year && params.code) {
      filters.push(sql`EXTRACT(YEAR FROM a.date_fin) = ${params.year}::int`);
    }
    if (!params.year && !params.code) {
      filters.push(sql`a.date_fin > now()`);
    }
    if (typeParam) {
      filters.push(sql`a.type = ${typeParam}`);
    }
    const query = sql`
      SELECT a.*, ST_AsGeoJSON(b.geom,6)::json as geom
      FROM ${raw(this.table)} a 
      LEFT JOIN ${raw(this.perim_table)} b on a.type = b.type AND left(a.code,9) = b.code 
      AND b.year = geo.get_latest_millesime()
      WHERE ${join(filters, " AND ")}
    `;
    const response = await this.pg.getClient().query(query);
    return response.rows;
  }
}
