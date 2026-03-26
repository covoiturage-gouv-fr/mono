import { provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
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
  private readonly table = "raw_zone.campaigns";
  private readonly perim_table = "trusted_zone.perimeters_agg";

  constructor(private pgConnection: DenoPostgresConnection) {}

  async getCampaigns(
    params: CampaignsParamsInterface,
  ): Promise<CampaignsResultInterface[]> {
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
    const rows = await this.pgConnection.query<CampaignsResultInterface>(query);
    return rows;
  }
}
