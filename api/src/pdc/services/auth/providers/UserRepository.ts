import { provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { fromObject, raw, RawValue } from "@/lib/pg/sql.ts";

export type LocalSiretUser = {
  email: string;
  role: string;
  operator_id: number | null;
  territory_id: number | null;
  siret: string | null;
  _id: number;
};

@provider()
export class UserRepository {
  public readonly table = "auth.users";
  public readonly territoryTable = "territory.territory_group";
  public readonly operatorTable = "operator.operators";
  public readonly companyTable = "company.companies";

  constructor(protected denoConnection: DenoPostgresConnection) {}

  async authenticateByEmail(email: string, data: Record<string, RawValue> = {}): Promise<LocalSiretUser | null> {
    if (!email) return null;
    const rows = await this.denoConnection.query<LocalSiretUser>(sql`
      SELECT
        u.email,
        u.role,
        u.operator_id,
        u.territory_id,
        COALESCE(o.siret, c.siret) as siret,
        u._id
      FROM ${raw(this.table)} u
      LEFT JOIN ${raw(this.territoryTable)} t
        ON t._id = u.territory_id
      LEFT JOIN ${raw(this.companyTable)} c on c._id = t.company_id
      LEFT JOIN ${raw(this.operatorTable)} o
        ON o._id = u.operator_id
      WHERE lower(u.email) = ${email.trim().toLowerCase()}
      LIMIT 1
    `);

    if (!rows.length) return null;

    // Update last_login_at and other data if provided
    const fields = fromObject({ ...data, last_login_at: "NOW()" });
    await this.denoConnection.query<{ role: string }>(sql`
      UPDATE ${raw(this.table)}
      SET ${fields}
      WHERE email = ${email}
    `);

    return rows[0];
  }
}
