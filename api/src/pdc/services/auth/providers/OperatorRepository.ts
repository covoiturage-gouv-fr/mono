import { provider } from "@/ilos/common/Decorators.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { raw } from "@/lib/pg/sql.ts";

@provider()
export class OperatorRepository {
  public readonly table = "operator.operators";

  constructor(protected denoConnection: DenoPostgresConnection) {}

  // Soft-deleted operators still exist: their leftover keys must stay readable and deletable
  async exists(operator_id: number, { active = false } = {}): Promise<boolean> {
    if (!operator_id) return false;

    const rows = await this.denoConnection.query<{ _id: number }>(sql`
      SELECT _id FROM ${raw(this.table)}
      WHERE _id = ${operator_id} ${active ? sql`AND deleted_at IS NULL` : sql``}
      LIMIT 1
    `);

    return rows.length > 0;
  }
}
