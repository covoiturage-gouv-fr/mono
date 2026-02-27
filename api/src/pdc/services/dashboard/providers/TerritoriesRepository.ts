import { provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { raw } from "@/lib/pg/sql.ts";
import {
  TerritoriesRepositoryInterface,
  TerritoriesRepositoryInterfaceResolver,
  TerritoryWithSiret,
} from "../interfaces/TerritoriesRepositoryInterface.ts";

@provider({
  identifier: TerritoriesRepositoryInterfaceResolver,
})
export class TerritoriesRepository implements TerritoriesRepositoryInterface {
  private readonly table = "territory.territory_group";
  private readonly companyTable = "company.companies";

  constructor(private pgConnection: DenoPostgresConnection) {}

  async getTerritoryById(id: number): Promise<TerritoryWithSiret | null> {
    const query = sql`
      SELECT
        tg._id as id,
        tg.name,
        c.siret
      FROM ${raw(this.table)} tg
      LEFT JOIN ${raw(this.companyTable)} c ON c._id = tg.company_id
      WHERE tg._id = ${id} AND tg.deleted_at IS NULL
    `;
    const rows = await this.pgConnection.query<TerritoryWithSiret>(query);
    return rows.length > 0 ? rows[0] : null;
  }
}
