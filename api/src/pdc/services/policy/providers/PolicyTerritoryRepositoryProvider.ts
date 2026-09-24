import { provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { raw } from "@/lib/pg/sql.ts";
import {
  ArrDescriptionInterface,
  PolicyTerritoryInterface,
  PolicyTerritoryRepositoryProviderInterfaceResolver,
  TerritoryCode,
} from "../interfaces/index.ts";

@provider({
  identifier: PolicyTerritoryRepositoryProviderInterfaceResolver,
})
export class PolicyTerritoryRepositoryProvider implements PolicyTerritoryRepositoryProviderInterfaceResolver {
  public readonly table = "policy.policy_territories";
  protected readonly perimetersTable = "geo.perimeters";

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
   * Resolve codes to arr over every millesime from fromYear onwards,
   * so that trips geocoded with the code of a since-merged commune still match.
   */
  async resolve(
    codes: TerritoryCode[],
    fromYear: number,
  ): Promise<{ arr: string[]; unknown: TerritoryCode[] }> {
    const rows = await this.pgConnection.query<{ type: string; code: string; arr: string[] }>(sql`
      WITH input AS (
        SELECT * FROM unnest(${codes.map((c) => c.type)}::varchar[], ${codes.map((c) => c.code)}::varchar[])
          AS t(type, code)
      ),
      years AS (
        SELECT LEAST(${fromYear}::smallint, MAX(year)) AS year FROM ${raw(this.perimetersTable)}
      )
      SELECT
        i.type,
        i.code,
        COALESCE(array_agg(DISTINCT p.arr) FILTER (WHERE p.arr IS NOT NULL), '{}') AS arr
      FROM input i
      CROSS JOIN years y
      LEFT JOIN ${raw(this.perimetersTable)} p
        ON p.year >= y.year
        AND i.code = CASE i.type
          WHEN 'arr' THEN p.arr
          WHEN 'com' THEN p.com
          WHEN 'epci' THEN p.epci
          WHEN 'aom' THEN p.aom
          WHEN 'dep' THEN p.dep
          WHEN 'reg' THEN p.reg
          WHEN 'reseau' THEN p.reseau::varchar
          WHEN 'country' THEN p.country
        END
      GROUP BY i.type, i.code
    `);

    return {
      arr: [...new Set(rows.flatMap((r) => r.arr))].sort(),
      unknown: rows.filter((r) => !r.arr.length).map(({ type, code }) => ({ type, code }) as TerritoryCode),
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
}
