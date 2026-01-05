import { provider } from "@/ilos/common/Decorators.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { raw } from "@/lib/pg/sql.ts";

@provider()
export class CampaignRepository {
  readonly table = "policy.policies";

  constructor(protected connection: DenoPostgresConnection) {}

  public async findByRange(start: Date, end: Date): Promise<number[]> {
    const campaigns = await this.connection.query<{ _id: number }>(sql`
      SELECT _id
      FROM ${raw(this.table)}
      WHERE NOT (end_date < ${start} OR start_date > ${end})
        AND status <> 'draft'
      ORDER BY 1
    `);

    return campaigns.map((c) => c._id);
  }
}
