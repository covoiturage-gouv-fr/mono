import { provider } from "../../../../ilos/common/Decorators.ts";
import { DenoPostgresConnection } from "../../../../ilos/connection-postgres/DenoPostgresConnection.ts";
import sql, { raw } from "../../../../lib/pg/sql.ts";
import { SerializedStoredMetadataInterface } from "../interfaces/engine/MetadataInterface.ts";
import { MetadataRepositoryProviderInterfaceResolver } from "../interfaces/providers/MetadataRepositoryProviderInterface.ts";

@provider({
  identifier: MetadataRepositoryProviderInterfaceResolver,
})
export class MetadataRepositoryProvider implements MetadataRepositoryProviderInterfaceResolver {
  public readonly table = "policy.policy_metas";

  constructor(protected connection: DenoPostgresConnection) {}

  protected async getSingle(
    policyId: number,
    key: string,
    datetime?: Date,
  ): Promise<SerializedStoredMetadataInterface> {
    const rows = await this.connection.query<SerializedStoredMetadataInterface>(sql`
      SELECT datetime, policy_id, key, value FROM ${raw(this.table)}
      WHERE policy_id = ${policyId}
        AND key = ${key}
        ${datetime ? sql`AND datetime <= ${datetime}` : sql``}
      ORDER BY datetime DESC, updated_at DESC
      LIMIT 1
    `);

    return rows[0];
  }

  async get(policyId: number, keys: string[], datetime?: Date): Promise<SerializedStoredMetadataInterface[]> {
    const result = [];
    for (const key of keys) {
      const r = await this.getSingle(policyId, key, datetime);
      if (r) {
        result.push(r);
      }
    }
    return result;
  }

  async set(data: SerializedStoredMetadataInterface[]): Promise<void> {
    const args: [number[], string[], number[], Date[]] = data.reduce(
      ([policyIds, keys, vals, dates], i) => {
        policyIds.push(i.policy_id);
        keys.push(i.key);
        vals.push(i.value);
        dates.push(i.datetime);
        return [policyIds, keys, vals, dates];
      },
      [[], [], [], []] as [number[], string[], number[], Date[]],
    );

    const [ids, keys, vals, dates] = args;
    const query = sql`
      INSERT INTO ${raw(this.table)} (policy_id, key, value, datetime)
      SELECT * FROM UNNEST(
        ${ids}::int[],
        ${keys}::varchar[],
        ${vals}::bigint[],
        ${dates}::timestamp[]
      )
    `;

    await this.connection.query(query);
  }

  async delete(policyId: number, from?: Date): Promise<void> {
    await this.connection.query(sql`
      DELETE FROM ${raw(this.table)}
      WHERE policy_id = ${policyId}
      ${from ? sql`AND datetime >= ${from}` : sql``}
    `);
  }
}
