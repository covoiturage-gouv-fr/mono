import { provider } from "@/ilos/common/Decorators.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { join, raw } from "@/lib/pg/sql.ts";
import {
  TerritoryCodeEnum,
  TerritorySelectorsInterface,
} from "@/pdc/services/territory/contracts/common/interfaces/TerritoryCodeInterface.ts";

interface PivotTerritorySelector {
  territory_group_id: number;
  selector_type: TerritoryCodeEnum;
  selector_value: string;
}

export abstract class TerritoryRepositoryInterfaceResolver {
  public async getTerritorySelectors(_territoryId: number): Promise<TerritorySelectorsInterface> {
    throw new Error("Not implemented");
  }
  public async getTerritoryPerimeterArr(_territoryId: number, _from: Date, _to: Date): Promise<string[] | null> {
    throw new Error("Not implemented");
  }
  public async getTerritoryName(_type: string, _code: string): Promise<string | null> {
    throw new Error("Not implemented");
  }
  public async getTerritoryNamesBatch(_type: string, _codes: string[]): Promise<string[]> {
    throw new Error("Not implemented");
  }
  public async getTerritoryGroupNames(_ids: number[]): Promise<string[]> {
    throw new Error("Not implemented");
  }
}

@provider({
  identifier: TerritoryRepositoryInterfaceResolver,
})
export class TerritoryRepository {
  public readonly territoryTable = "territory.territory_group";
  public readonly pivotTable = "territory.territory_group_selector";
  public readonly geoTable = "geo.perimeters";
  public readonly perimeterTable = "territory.territory_perimeters";

  constructor(protected connection: DenoPostgresConnection) {}

  /**
   * Get the territory selectors for a given territory
   *
   * @param territoryId
   * @returns
   */
  public async getTerritorySelectors(territoryId: number): Promise<TerritorySelectorsInterface> {
    const q = sql`SELECT * FROM ${raw(this.pivotTable)} WHERE territory_group_id = ${territoryId}`;
    const rows = await this.connection.query<PivotTerritorySelector>(q);

    return rows.length ? this.formatSelectors(rows) : {};
  }

  /**
   * Arrondissements of a territory over [from, to), for territories with perimeter versions
   * (custom territories). `null` when the territory has none: it follows its selectors.
   *
   * `get_arr_range` unions the versions overlapping the period: an export over a perimeter
   * change includes the trips of both versions.
   */
  public async getTerritoryPerimeterArr(territoryId: number, from: Date, to: Date): Promise<string[] | null> {
    const q = sql`
      SELECT array_agg(r.arr ORDER BY r.arr) AS arr
      FROM territory.get_arr_range(${territoryId}::int, ${from}::timestamptz, ${to}::timestamptz) r
      WHERE EXISTS (SELECT 1 FROM ${raw(this.perimeterTable)} WHERE territory_id = ${territoryId})
    `;
    const rows = await this.connection.query<{ arr: string[] | null }>(q);
    return rows[0]?.arr ?? null;
  }

  /**
   * Convert the pivot table rows to a TerritorySelectorsInterface
   *
   * @param rows
   */
  private formatSelectors(rows: PivotTerritorySelector[]): TerritorySelectorsInterface {
    return rows.reduce((acc, row) => {
      acc[row.selector_type as keyof TerritorySelectorsInterface] = [
        ...(acc[row.selector_type as keyof TerritorySelectorsInterface] || []),
        row.selector_value,
      ];
      return acc;
    }, {} as TerritorySelectorsInterface);
  }

  /**
   * Get the territory name from type and code
   *
   * @param type - The territory type (com, epci, aom, etc.)
   * @param code - The territory code
   * @returns The territory name or null if not found
   */
  public async getTerritoryName(type: string, code: string): Promise<string | null> {
    const validTypes = Object.values(TerritoryCodeEnum) as string[];
    if (!validTypes.includes(type)) return null;

    const codeColumn = type;
    const labelColumn = `l_${type}`;

    const q = sql`
      SELECT ${raw(labelColumn)} as name
      FROM ${raw(this.geoTable)}
      WHERE ${raw(codeColumn)} = ${code}
      AND year = geo.get_latest_millesime()
      LIMIT 1
    `;

    const rows = await this.connection.query<{ name: string }>(q);
    return rows.length > 0 ? rows[0].name : null;
  }

  public async getTerritoryNamesBatch(type: string, codes: string[]): Promise<string[]> {
    if (!codes.length) return [];

    const validTypes = Object.values(TerritoryCodeEnum) as string[];
    if (!validTypes.includes(type)) return [];

    const codeColumn = type;
    const labelColumn = `l_${type}`;

    const q = sql`
      SELECT DISTINCT ${raw(labelColumn)} as name
      FROM ${raw(this.geoTable)}
      WHERE ${raw(codeColumn)} IN (${join(codes.map((c) => sql`${c}`))})
      AND year = geo.get_latest_millesime()
    `;

    const rows = await this.connection.query<{ name: string }>(q);
    return rows.map((r) => r.name);
  }

  public async getTerritoryGroupNames(ids: number[]): Promise<string[]> {
    if (!ids.length) return [];
    const q = sql`
      SELECT name
      FROM ${raw(this.territoryTable)}
      WHERE _id IN (${join(ids.map((id) => sql`${id}::int`))})
      ORDER BY name
    `;
    const rows = await this.connection.query<{ name: string }>(q);
    return rows.map((r) => r.name);
  }
}
