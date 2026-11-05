import { provider } from "@/ilos/common/Decorators.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { raw } from "@/lib/pg/sql.ts";

@provider()
export class OperatorRepository {
  public readonly table = "operator.operators";

  constructor(protected denoConnection: DenoPostgresConnection) {}

  async exists(operator_id: number): Promise<boolean> {
    if (!operator_id) return false;

    const rows = await this.denoConnection.query<{ _id: number }>(sql`
      SELECT _id FROM ${raw(this.table)} WHERE _id = ${operator_id} AND deleted_at IS NULL LIMIT 1
    `);

    return rows.length > 0;
  }
}
