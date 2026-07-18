import { provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { raw } from "@/lib/pg/sql.ts";

// Périmètre autorisé d'un utilisateur (operator_id XOR territory_id).
export type UserScope = {
  operator_id?: number;
  territory_id?: number;
  label: string;
  siret?: string;
};

// Scope par défaut : identifiants seuls, pour seeder le contexte actif au login.
export type DefaultScope = { operator_id?: number; territory_id?: number };

type ScopeRow = {
  operator_id: number | null;
  territory_id: number | null;
  label: string | null;
  siret: string | null;
};

@provider()
export class UserScopeRepository {
  public readonly table = "auth.user_scopes";
  public readonly territoryTable = "territory.territory_group";
  public readonly operatorTable = "operator.operators";
  public readonly companyTable = "company.companies";

  constructor(protected denoConnection: DenoPostgresConnection) {}

  // Liste des périmètres autorisés, défaut en tête, avec libellé/SIRET d'affichage.
  async findByUser(userId: number): Promise<UserScope[]> {
    const rows = await this.denoConnection.query<ScopeRow>(sql`
      SELECT
        us.operator_id,
        us.territory_id,
        COALESCE(o.name, t.name) AS label,
        COALESCE(o.siret, c.siret) AS siret
      FROM ${raw(this.table)} us
      LEFT JOIN ${raw(this.operatorTable)} o ON o._id = us.operator_id
      LEFT JOIN ${raw(this.territoryTable)} t ON t._id = us.territory_id
      LEFT JOIN ${raw(this.companyTable)} c ON c._id = t.company_id
      WHERE us.user_id = ${userId}
      ORDER BY us.is_default DESC, us._id ASC
    `);

    return rows.map((r) => this.toScope(r));
  }

  // Scope par défaut (is_default), fallback MIN(_id) pour ne jamais casser le login.
  async findDefault(userId: number): Promise<DefaultScope | null> {
    const rows = await this.denoConnection.query<{ operator_id: number | null; territory_id: number | null }>(sql`
      SELECT operator_id, territory_id
      FROM ${raw(this.table)}
      WHERE user_id = ${userId}
      ORDER BY is_default DESC, _id ASC
      LIMIT 1
    `);
    if (!rows.length) return null;
    return this.toDefault(rows[0]);
  }

  // Revalidation de la bascule : match strict sur territory_id (jamais operator_id → anti-IDOR).
  async userHasTerritory(userId: number, territoryId: number): Promise<boolean> {
    const rows = await this.denoConnection.query<{ one: number }>(sql`
      SELECT 1 AS one
      FROM ${raw(this.table)}
      WHERE user_id = ${userId} AND territory_id = ${territoryId}
      LIMIT 1
    `);
    return rows.length > 0;
  }

  // Ajoute un territoire ; si défaut, dégrade l'ancien défaut dans la même transaction.
  async addTerritory(userId: number, territoryId: number, isDefault = false): Promise<void> {
    if (!isDefault) {
      await this.denoConnection.query(sql`
        INSERT INTO ${raw(this.table)} (user_id, territory_id, is_default)
        VALUES (${userId}, ${territoryId}, false)
      `);
      return;
    }

    using client = await this.denoConnection.getClient().connect();
    try {
      await client.queryArray("BEGIN");
      await client.queryObject({
        text: `UPDATE ${this.table} SET is_default = false WHERE user_id = $1 AND is_default`,
        args: [userId],
      });
      await client.queryObject({
        text: `INSERT INTO ${this.table} (user_id, territory_id, is_default) VALUES ($1, $2, true)`,
        args: [userId, territoryId],
      });
      await client.queryArray("COMMIT");
    } catch (e) {
      await client.queryArray("ROLLBACK");
      throw e;
    }
  }

  // Attribue l'unique opérateur (1:1) comme scope par défaut.
  async setOperator(userId: number, operatorId: number): Promise<void> {
    await this.denoConnection.query(sql`
      INSERT INTO ${raw(this.table)} (user_id, operator_id, is_default)
      VALUES (${userId}, ${operatorId}, true)
    `);
  }

  // Retire un territoire ; promeut MIN(_id) si le défaut partait ; refuse le dernier scope.
  async removeTerritory(userId: number, territoryId: number): Promise<void> {
    using client = await this.denoConnection.getClient().connect();
    try {
      await client.queryArray("BEGIN");

      const { rows: targets } = await client.queryObject<{ _id: number; is_default: boolean }>({
        text: `SELECT _id, is_default FROM ${this.table} WHERE user_id = $1 AND territory_id = $2 FOR UPDATE`,
        args: [userId, territoryId],
      });
      if (!targets.length) {
        await client.queryArray("ROLLBACK");
        return;
      }

      const { rows: counts } = await client.queryObject<{ n: number }>({
        text: `SELECT count(*)::int AS n FROM ${this.table} WHERE user_id = $1`,
        args: [userId],
      });
      if (counts[0].n <= 1) {
        throw new Error("Impossible de retirer le dernier périmètre d'un utilisateur");
      }

      await client.queryObject({
        text: `DELETE FROM ${this.table} WHERE _id = $1`,
        args: [targets[0]._id],
      });

      if (targets[0].is_default) {
        await client.queryObject({
          text:
            `UPDATE ${this.table} SET is_default = true WHERE _id = (SELECT MIN(_id) FROM ${this.table} WHERE user_id = $1)`,
          args: [userId],
        });
      }

      await client.queryArray("COMMIT");
    } catch (e) {
      await client.queryArray("ROLLBACK");
      throw e;
    }
  }

  private toDefault(r: { operator_id: number | null; territory_id: number | null }): DefaultScope {
    return r.operator_id !== null ? { operator_id: r.operator_id } : { territory_id: r.territory_id as number };
  }

  private toScope(r: ScopeRow): UserScope {
    const base = this.toDefault(r) as UserScope;
    base.label = r.label ?? "";
    if (r.siret !== null) base.siret = r.siret;
    return base;
  }
}
