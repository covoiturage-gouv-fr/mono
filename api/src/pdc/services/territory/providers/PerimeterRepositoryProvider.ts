import { provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { raw } from "@/lib/pg/sql.ts";
import { TerritorySelectorsInterface } from "../contracts/common/interfaces/TerritoryCodeInterface.ts";
import { TerritoryPerimeterInterface } from "../contracts/common/interfaces/TerritoryPerimeterInterface.ts";
import {
  ArrDescriptionInterface,
  ComEvolutionInterface,
  OwnedPolicyInterface,
  PerimeterRepositoryProviderInterfaceResolver,
} from "../interfaces/PerimeterRepositoryProviderInterface.ts";

@provider({
  identifier: PerimeterRepositoryProviderInterfaceResolver,
})
export class PerimeterRepositoryProvider implements PerimeterRepositoryProviderInterfaceResolver {
  public readonly table = "territory.territory_perimeters";
  protected readonly territoryTable = "territory.territory_group";
  protected readonly companyTable = "company.companies";
  protected readonly policyTable = "policy.policies";
  protected readonly perimetersTable = "geo.perimeters";
  protected readonly evolutionTable = "geo.com_evolution";
  protected readonly getArrBySelectorsFunction = "geo.get_arr_by_selectors";
  protected readonly getArrFunction = "territory.get_arr";

  constructor(protected pgConnection: DenoPostgresConnection) {}

  async findTerritory(territory_id: number): Promise<{ _id: number; name: string } | undefined> {
    const rows = await this.pgConnection.query<{ _id: number; name: string }>(sql`
      SELECT _id, name
      FROM ${raw(this.territoryTable)}
      WHERE _id = ${territory_id} AND deleted_at IS NULL
    `);
    return rows[0];
  }

  async findTerritoryByName(name: string): Promise<{ _id: number; name: string } | undefined> {
    const rows = await this.pgConnection.query<{ _id: number; name: string }>(sql`
      SELECT _id, name
      FROM ${raw(this.territoryTable)}
      WHERE lower(name) = lower(trim(${name})) AND deleted_at IS NULL
      ORDER BY _id
      LIMIT 1
    `);
    return rows[0];
  }

  async createTerritory(
    name: string,
    siret: string | undefined,
    data: Omit<TerritoryPerimeterInterface, "version">,
  ): Promise<number> {
    let company_id: number | null = null;
    if (siret) {
      const companies = await this.pgConnection.query<{ _id: number }>(sql`
        SELECT _id FROM ${raw(this.companyTable)} WHERE siret = ${siret}
      `);
      if (!companies.length) {
        throw new Error(`Aucune entreprise pour le SIRET ${siret} (just api company:fetch ${siret})`);
      }
      company_id = companies[0]._id;
    }

    // single statement: a rejected version does not leave a territory without perimeter
    const rows = await this.pgConnection.query<{ territory_id: number }>(sql`
      WITH territory AS (
        INSERT INTO ${raw(this.territoryTable)} (name, company_id)
        VALUES (${name}, ${company_id})
        RETURNING _id
      )
      INSERT INTO ${raw(this.table)} (territory_id, version, arr, valid_from, valid_to)
      SELECT
        _id,
        1,
        ${data.arr}::varchar[],
        ${data.valid_from}::timestamptz,
        ${data.valid_to}::timestamptz
      FROM territory
      RETURNING territory_id
    `);
    return rows[0].territory_id;
  }

  async findTerritoriesWithVersions(): Promise<number[]> {
    const rows = await this.pgConnection.query<{ territory_id: number }>(sql`
      SELECT DISTINCT tp.territory_id
      FROM ${raw(this.table)} tp
      JOIN ${raw(this.territoryTable)} tg ON tg._id = tp.territory_id AND tg.deleted_at IS NULL
      ORDER BY tp.territory_id
    `);
    return rows.map((r) => r.territory_id);
  }

  async findByTerritory(territory_id: number): Promise<TerritoryPerimeterInterface[]> {
    return await this.pgConnection.query<TerritoryPerimeterInterface>(sql`
      SELECT version, arr, valid_from, valid_to
      FROM ${raw(this.table)}
      WHERE territory_id = ${territory_id}
      ORDER BY version
    `);
  }

  async create(
    territory_id: number,
    data: Omit<TerritoryPerimeterInterface, "version">,
  ): Promise<TerritoryPerimeterInterface> {
    // concurrent writers compute the same version: the UNIQUE constraint rejects the second one
    const rows = await this.pgConnection.query<TerritoryPerimeterInterface>(sql`
      INSERT INTO ${raw(this.table)} (territory_id, version, arr, valid_from, valid_to)
      SELECT
        ${territory_id}::int,
        COALESCE(MAX(version), 0) + 1,
        ${data.arr}::varchar[],
        ${data.valid_from}::timestamptz,
        ${data.valid_to}::timestamptz
      FROM ${raw(this.table)}
      WHERE territory_id = ${territory_id}
      RETURNING version, arr, valid_from, valid_to
    `);
    return rows[0];
  }

  async getArr(territory_id: number, at: Date): Promise<string[]> {
    const rows = await this.pgConnection.query<{ arr: string }>(sql`
      SELECT arr FROM ${raw(this.getArrFunction)}(${territory_id}::int, ${at}::timestamptz) ORDER BY arr
    `);
    return rows.map((r) => r.arr);
  }

  /**
   * Resolve selectors against every millesime still loaded, so that trips
   * geocoded with the previous millesime keep matching after a merge.
   */
  async resolve(selectors: TerritorySelectorsInterface): Promise<{ arr: string[]; unknown: string[] }> {
    const pairs = (Object.entries(selectors) as [string, string[] | undefined][])
      .flatMap(([type, codes]) => (codes ?? []).map((code) => [type, code]));
    const rows = await this.pgConnection.query<{ selector_type: string; selector_value: string; arr: string }>(sql`
      SELECT DISTINCT r.selector_type, r.selector_value, r.arr
      FROM (SELECT DISTINCT year FROM ${raw(this.perimetersTable)}) y
      CROSS JOIN LATERAL ${raw(this.getArrBySelectorsFunction)}(
        ${pairs.map(([t]) => t)}::varchar[],
        ${pairs.map(([, c]) => c)}::varchar[],
        y.year
      ) r
    `);

    const found = new Set(rows.map((r) => `${r.selector_type}:${r.selector_value}`));
    return {
      arr: [...new Set(rows.map((r) => r.arr))].sort(),
      unknown: pairs.map(([t, c]) => `${t}:${c}`).filter((k) => !found.has(k)),
    };
  }

  async describe(arr: string[]): Promise<ArrDescriptionInterface[]> {
    return await this.pgConnection.query<ArrDescriptionInterface>(sql`
      SELECT DISTINCT ON (arr) arr, l_arr AS label, pop
      FROM ${raw(this.perimetersTable)}
      WHERE arr = ANY(${arr}::varchar[])
      ORDER BY arr, year DESC
    `);
  }

  async findEvolutions(): Promise<ComEvolutionInterface[]> {
    return await this.pgConnection.query<ComEvolutionInterface>(sql`
      SELECT DISTINCT old_com, new_com
      FROM ${raw(this.evolutionTable)}
      WHERE old_com IS NOT NULL AND new_com IS NOT NULL AND old_com <> new_com
    `);
  }

  async findPolicies(territory_id: number): Promise<OwnedPolicyInterface[]> {
    return await this.pgConnection.query<OwnedPolicyInterface>(sql`
      SELECT _id, name, status
      FROM ${raw(this.policyTable)}
      WHERE territory_id = ${territory_id} AND deleted_at IS NULL
      ORDER BY _id
    `);
  }
}
