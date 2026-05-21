import { NotFoundException, provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import { logger } from "@/lib/logger/index.ts";
import sql, { raw } from "@/lib/pg/sql.ts";
import {
  TerritoryCodeEnum,
  TerritoryCodeInterface,
  TerritoryRepositoryProviderInterface,
  TerritoryRepositoryProviderInterfaceResolver,
  TerritorySelectorsInterface,
} from "../interfaces/index.ts";

@provider({
  identifier: TerritoryRepositoryProviderInterfaceResolver,
})
export class TerritoryRepositoryProvider implements TerritoryRepositoryProviderInterface {
  protected readonly getTerritorySelectorFn = "territory.get_selector_by_territory_id";
  protected readonly getByPointFunction = "geo.get_latest_by_point";
  protected readonly getBySelectorFunction = "policy.get_territory_id_by_selector";
  protected readonly territoryGroupTable = "territory.territory_group";
  protected readonly territorySelectorTable = "territory.territory_group_selector";
  protected readonly operatorTable = "operator.operators";
  protected readonly companyTable = "company.companies";

  constructor(protected pgConnection: DenoPostgresConnection) {}

  async findByPoint(
    { lon, lat }: { lon: number; lat: number },
  ): Promise<TerritoryCodeInterface> {
    try {
      const rows = await this.pgConnection.query<TerritoryCodeInterface>(sql`
        SELECT * FROM ${raw(this.getByPointFunction)}(${lon}::float, ${lat}::float)
      `);

      if (!rows.length) {
        throw new NotFoundException();
      }
      return rows[0];
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      logger.error(message, e);
      return null;
    }
  }

  async findUUIDByOperatorId(_id: number): Promise<string> {
    const rows = await this.pgConnection.query<{ _id: number; uuid: string }>(sql`
      SELECT
        o._id, c.uuid
      FROM ${raw(this.operatorTable)} AS o
      LEFT JOIN ${raw(this.companyTable)} AS c
        ON c._id = o.company_id
      WHERE o._id = ${_id} LIMIT 1
    `);

    if (!rows.length) {
      throw new NotFoundException();
    }

    return rows[0]?.uuid;
  }

  async findUUIDById(
    _id: number | number[],
  ): Promise<{ _id: number; uuid: string }[]> {
    const idFilter = Array.isArray(_id)
      ? sql`t._id = ANY(${_id})`
      : sql`t._id = ${_id}`;

    return await this.pgConnection.query<{ _id: number; uuid: string }>(sql`
      SELECT
        t._id, c.uuid
      FROM ${raw(this.territoryGroupTable)} AS t
      LEFT JOIN ${raw(this.companyTable)} AS c
        ON c._id = t.company_id
      WHERE ${idFilter}
    `);
  }

  async findBySelector(
    data: Partial<TerritoryCodeInterface>,
  ): Promise<number[]> {
    const arr = data[TerritoryCodeEnum.Arr] || data[TerritoryCodeEnum.City];
    const mobility = data[TerritoryCodeEnum.Mobility];
    const rows = await this.pgConnection.query<{ _id: number }>(sql`
      SELECT _id FROM ${raw(this.getBySelectorFunction)}(${arr}::varchar, ${mobility}::varchar)
    `);
    return rows.map((r) => r._id);
  }

  async findSelectorFromId(id: number): Promise<TerritorySelectorsInterface> {
    const rows = await this.pgConnection.query<{ selector: TerritorySelectorsInterface }>(sql`
      SELECT * FROM ${raw(this.getTerritorySelectorFn)}(${[id]})
    `);
    if (rows.length !== 1) {
      throw new NotFoundException();
    }
    return rows[0].selector;
  }
}
