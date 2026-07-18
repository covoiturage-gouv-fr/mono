import { provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { fromObject, raw, RawValue } from "@/lib/pg/sql.ts";
import { UserScope, UserScopeRepository } from "@/pdc/services/auth/providers/UserScopeRepository.ts";

export type LocalSiretUser = {
  email: string;
  role: string;
  login_siren: string | null;
  operator_id: number | null;
  territory_id: number | null;
  siret: string | null;
  _id: number;
  analytics_id: string;
  organisation: string | null;
  scopes: UserScope[];
};

// Identité de base lue sur auth.users ; les périmètres viennent de user_scopes.
type UserIdentityRow = {
  email: string;
  role: string;
  login_siren: string | null;
  _id: number;
  analytics_id: string;
};

@provider()
export class UserRepository {
  public readonly table = "auth.users";

  constructor(
    protected denoConnection: DenoPostgresConnection,
    protected userScopeRepository: UserScopeRepository,
  ) {}

  async authenticateByEmail(email: string, data: Record<string, RawValue> = {}): Promise<LocalSiretUser | null> {
    if (!email) return null;
    const rows = await this.denoConnection.query<UserIdentityRow>(sql`
      SELECT
        u.email,
        u.role,
        u.login_siren,
        u._id,
        u.analytics_id
      FROM ${raw(this.table)} u
      WHERE lower(u.email) = ${email.trim().toLowerCase()}
      LIMIT 1
    `);

    if (!rows.length) return null;
    const identity = rows[0];

    // Update last_login_at and other data if provided
    const fields = fromObject({ ...data, last_login_at: "NOW()", deletion_warned_at: raw("NULL") });
    await this.denoConnection.query<{ role: string }>(sql`
      UPDATE ${raw(this.table)}
      SET ${fields}
      WHERE email = ${email}
    `);

    // Périmètres : liste complète (dropdown) + défaut (contexte actif), fallback MIN(_id) porté par le repo.
    const scopes = await this.userScopeRepository.findByUser(identity._id);
    const def = await this.userScopeRepository.findDefault(identity._id);
    const active = scopes.find((s) =>
      def?.operator_id != null
        ? s.operator_id === def.operator_id
        : def?.territory_id != null && s.territory_id === def.territory_id
    ) ?? scopes[0];

    return {
      email: identity.email,
      role: identity.role,
      login_siren: identity.login_siren,
      _id: identity._id,
      analytics_id: identity.analytics_id,
      operator_id: def?.operator_id ?? null,
      territory_id: def?.territory_id ?? null,
      siret: active?.siret ?? null,
      organisation: active?.label ?? null,
      scopes,
    };
  }
}
