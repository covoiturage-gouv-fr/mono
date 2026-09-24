import { provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { raw } from "@/lib/pg/sql.ts";
import {
  ArrDescriptionInterface,
  ComEvolutionInterface,
  PolicyTerritoryInterface,
  PolicyTerritoryRepositoryProviderInterfaceResolver,
  TerritorySelectorsInterface,
} from "../interfaces/index.ts";

@provider({
  identifier: PolicyTerritoryRepositoryProviderInterfaceResolver,
})
export class PolicyTerritoryRepositoryProvider implements PolicyTerritoryRepositoryProviderInterfaceResolver {
  public readonly table = "policy.policy_territories";
  protected readonly perimetersTable = "geo.perimeters";
  protected readonly evolutionTable = "geo.com_evolution";
  protected readonly getArrFunction = "geo.get_arr_by_selectors";

  constructor(protected pgConnection: DenoPostgresConnection) {}

  async findByPolicy(policy_id: number): Promise<PolicyTerritoryInterface[]> {
    return await this.pgConnection.query<PolicyTerritoryInterface>(sql`
      SELECT version, arr, valid_from, valid_to
      FROM ${raw(this.table)}
      WHERE policy_id = ${policy_id}
      ORDER BY version
    `);
  }

  async create(
    policy_id: number,
    data: Omit<PolicyTerritoryInterface, "version">,
  ): Promise<PolicyTerritoryInterface> {
    // concurrent writers compute the same version: the UNIQUE constraint rejects the second one
    const rows = await this.pgConnection.query<PolicyTerritoryInterface>(sql`
      INSERT INTO ${raw(this.table)} (policy_id, version, arr, valid_from, valid_to)
      SELECT
        ${policy_id}::int,
        COALESCE(MAX(version), 0) + 1,
        ${data.arr}::varchar[],
        ${data.valid_from}::timestamptz,
        ${data.valid_to}::timestamptz
      FROM ${raw(this.table)}
      WHERE policy_id = ${policy_id}
      RETURNING version, arr, valid_from, valid_to
    `);
    return rows[0];
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
      CROSS JOIN LATERAL ${raw(this.getArrFunction)}(
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
}
