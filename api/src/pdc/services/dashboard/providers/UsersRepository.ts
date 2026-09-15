import { ConflictException, NotFoundException, provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { join, raw } from "@/lib/pg/sql.ts";
import { UserScopeRepository } from "@/pdc/services/auth/providers/UserScopeRepository.ts";
import { UserResult } from "@/pdc/services/dashboard/actions/users/UsersAction.ts";
import {
  CreateUserDataInterface,
  CreateUserResultInterface,
  DeleteUserParamsInterface,
  DeleteUserResultInterface,
  InactiveUserToWarn,
  UpdateUserDataInterface,
  UpdateUserResultInterface,
  UsersParamsInterface,
  UsersRepositoryInterface,
  UsersRepositoryInterfaceResolver,
  UsersResultInterface,
} from "@/pdc/services/dashboard/interfaces/UsersRepositoryInterface.ts";

// Entrée de reconciliation des périmètres (formulaire, ou colonnes legacy d'un appel API direct).
type ScopeSeedData = {
  operator_id?: number | null;
  territory_id?: number | null;
  scopes?: Array<{ territory_id: number; is_default?: boolean }>;
};

@provider({
  identifier: UsersRepositoryInterfaceResolver,
})
export class UsersRepository implements UsersRepositoryInterface {
  private readonly table = "auth.users";
  private readonly tableScopes = "auth.user_scopes";
  private readonly tableTerritory = "territory.territory_group";
  private readonly tableOperator = "operator.operators";

  constructor(
    private pgConnection: DenoPostgresConnection,
    private userScopeRepository: UserScopeRepository,
  ) {}

  async getUsers(params: UsersParamsInterface): Promise<UsersResultInterface> {
    // Filtrage des périmètres via le pivot user_scopes ; LEFT JOIN pour ne pas perdre
    // les comptes sans scope (ex : registry.admin) lorsque le caller n'impose pas de filtre.
    const filters = [sql`TRUE`];
    if (params.id) {
      filters.push(sql`users._id = ${params.id}`);
    }
    if (params.operator_id) {
      filters.push(sql`us.operator_id = ${params.operator_id}`);
    }
    if (params.territory_id) {
      filters.push(sql`us.territory_id = ${params.territory_id}`);
    }
    if (params.search) {
      filters.push(sql`(
        users.firstname ILIKE ${`%${params.search}%`}
        OR users.lastname ILIKE ${`%${params.search}%`}
        OR users.email ILIKE ${`%${params.search}%`}
        OR o.name ILIKE ${`%${params.search}%`}
        OR tg.name ILIKE ${`%${params.search}%`}
      )`);
    }
    const limit = params.limit || 25;
    const page = params.page || 1;
    const offset = (page - 1) * limit;
    const searchJoin = params.search
      ? sql`LEFT JOIN ${raw(this.tableTerritory)} tg ON tg._id = us.territory_id LEFT JOIN ${
        raw(this.tableOperator)
      } o ON o._id = us.operator_id`
      : sql``;
    // GROUP BY users._id (PK) : un user multi-territoires n'apparaît qu'une fois ;
    // le périmètre exposé est son scope par défaut dans le pivot.
    const query = sql`
      SELECT
        users._id as id,
        users.firstname,
        users.lastname,
        users.email,
        (ARRAY_AGG(us.operator_id ORDER BY us.is_default DESC, us._id ASC))[1] AS operator_id,
        (ARRAY_AGG(us.territory_id ORDER BY us.is_default DESC, us._id ASC))[1] AS territory_id,
        users.role,
        users.login_siren,
        -- Sous-requêtes : le WHERE dégrade le LEFT JOIN en jointure interne, il ne verrait
        -- que les scopes du caller.
        (SELECT COUNT(*) FROM ${raw(this.tableScopes)} s WHERE s.user_id = users._id)::int AS scopes_count,
        -- Périmètres complets : sans eux le formulaire d'édition réécrirait le compte avec le seul défaut.
        COALESCE((
          SELECT json_agg(json_build_object('territory_id', s.territory_id, 'is_default', s.is_default)
                          ORDER BY s.is_default DESC, s._id ASC)
          FROM ${raw(this.tableScopes)} s
          WHERE s.user_id = users._id AND s.territory_id IS NOT NULL
        ), '[]'::json) AS scopes
      FROM ${raw(this.table)} AS users
      LEFT JOIN ${raw(this.tableScopes)} us ON us.user_id = users._id
      ${searchJoin}
      WHERE ${join(filters, " AND ")}
      GROUP BY users._id
      ORDER BY users._id
      LIMIT ${limit} OFFSET ${offset}
    `;

    const rows = await this.pgConnection.query<UserResult>(query);
    // Total = nombre de users distincts (le JOIN pivot démultiplie les lignes).
    const countQuery = sql`
      SELECT COUNT(DISTINCT users._id) as total
      FROM ${raw(this.table)} AS users
      LEFT JOIN ${raw(this.tableScopes)} us ON us.user_id = users._id
      ${searchJoin}
      WHERE ${join(filters, " AND ")}
    `;
    const countResponse = await this.pgConnection.query<{ total: number }>(countQuery);
    const total = countResponse[0].total;
    return {
      meta: {
        total,
        page: page,
        totalPages: Math.ceil(total / limit),
      },
      data: rows,
    };
  }

  async createUser(data: CreateUserDataInterface): Promise<CreateUserResultInterface> {
    const query = sql`
      INSERT INTO ${raw(this.table)} (
        firstname, lastname, email, role, login_siren
      ) VALUES (
        ${data.firstname}, ${data.lastname}, ${data.email}, ${data.role}, ${data.login_siren ?? null}
      )
      RETURNING
        _id, created_at, firstname, lastname, email, role
    `;
    const rows = await this.pgConnection.query<{ _id: number }>(query);
    if (rows.length !== 1) {
      throw new Error(`Unable to create user ${data}`);
    }
    // Source de vérité : le pivot user_scopes (défaut = scope initial).
    await this.seedScopes(rows[0]._id, data);
    return {
      success: true,
      message: `user ${JSON.stringify(rows[0])} created`,
    };
  }

  // Périmètres demandés, défaut en tête (drapeau is_default, à défaut le premier de la liste).
  private requestedTerritories(data: ScopeSeedData): number[] {
    const scopes = data.scopes ?? (data.territory_id ? [{ territory_id: data.territory_id }] : []);
    const ordered = [...scopes].sort((a, b) => Number(b.is_default ?? false) - Number(a.is_default ?? false));
    return [...new Set(ordered.map((s) => s.territory_id))];
  }

  // Reconcilie user_scopes avec le formulaire, via UserScopeRepository.
  private async seedScopes(userId: number, data: ScopeSeedData): Promise<void> {
    // Reset idempotent (no-op à la création, resynchronise à l'update).
    await this.pgConnection.query(sql`
      DELETE FROM ${raw(this.tableScopes)} WHERE user_id = ${userId}
    `);

    if (data.operator_id) {
      await this.userScopeRepository.setOperator(userId, data.operator_id);
      return;
    }

    const territories = this.requestedTerritories(data);
    for (let i = 0; i < territories.length; i++) {
      await this.userScopeRepository.addTerritory(userId, territories[i], i === 0);
    }
  }

  async deleteUser(
    params: DeleteUserParamsInterface & { operator_id?: number; territory_id?: number },
  ): Promise<DeleteUserResultInterface> {
    const checks = [];
    if (params.operator_id) {
      checks.push(sql`us.operator_id = ${params.operator_id}`);
    }
    if (params.territory_id) {
      checks.push(sql`us.territory_id = ${params.territory_id}`);
    }

    if (checks.length) {
      const counts = await this.pgConnection.query<{ total: number; granted: number }>(sql`
        SELECT
          count(*)::int AS total,
          count(*) FILTER (WHERE ${join(checks, " OR ")})::int AS granted
        FROM ${raw(this.tableScopes)} us
        WHERE us.user_id = ${params.id}
      `);
      // Périmètre non accordé (ou compte inconnu) : on refuse.
      if (!counts.length || counts[0].granted < checks.length) {
        throw new NotFoundException();
      }
      if (counts[0].total > 1) {
        // Un scope opérateur est 1:1 : multi-périmètres ici = incohérent, on refuse.
        if (!params.territory_id) {
          throw new ConflictException(`user ${params.id} has multiple scopes`);
        }
        await this.userScopeRepository.removeTerritory(params.id, params.territory_id);
        return {
          success: true,
          message: `territory ${params.territory_id} released for user ${params.id}`,
          outcome: "scope_released",
        };
      }
    }

    const query = sql`
      DELETE FROM ${raw(this.table)} AS users
      WHERE users._id = ${params.id}
      RETURNING users._id
    `;
    const rows = await this.pgConnection.query(query);
    if (rows.length !== 1) {
      throw new NotFoundException();
    }
    return { success: true, message: `user ${params.id} deleted`, outcome: "user_deleted" };
  }

  async updateUser(data: UpdateUserDataInterface): Promise<UpdateUserResultInterface> {
    const query = sql`
      UPDATE ${raw(this.table)}
      SET
        firstname = ${data.firstname},
        lastname = ${data.lastname},
        email = ${data.email},
        role = ${data.role},
        login_siren = ${data.login_siren ?? null},
        updated_at = now()
      WHERE _id = ${data.id}
      RETURNING _id, updated_at, firstname, lastname, email, role
    `;
    const rows = await this.pgConnection.query<UpdateUserResultInterface>(query);
    if (rows.length !== 1) {
      throw new Error(`Unable to update user with id ${data.id}`);
    }
    // Pivot touché seulement si le formulaire porte des périmètres (ou bascule vers un opérateur) :
    // un admin de territoire n'en envoie pas, et ne doit pas amputer un compte multi-territoires.
    if (data.scopes?.length || data.operator_id) {
      await this.seedScopes(data.id, data);
    }
    return {
      success: true,
      message: `User ${JSON.stringify(rows[0])} updated`,
    };
  }

  async findUsersToWarn(inactivity: string): Promise<InactiveUserToWarn[]> {
    return await this.pgConnection.query<InactiveUserToWarn>(sql`
      SELECT _id, email, firstname, lastname
      FROM ${raw(this.table)}
      WHERE last_login_at < now() - ${inactivity}::interval
        AND deletion_warned_at IS NULL
        AND role NOT LIKE 'registry.%'
    `);
  }

  async markUserWarned(id: number): Promise<void> {
    await this.pgConnection.query(sql`
      UPDATE ${raw(this.table)} SET deletion_warned_at = now() WHERE _id = ${id}
    `);
  }

  async findUsersToDelete(grace: string): Promise<Array<{ _id: number }>> {
    return await this.pgConnection.query<{ _id: number }>(sql`
      SELECT _id
      FROM ${raw(this.table)}
      WHERE deletion_warned_at < now() - ${grace}::interval
        AND last_login_at < deletion_warned_at
        AND role NOT LIKE 'registry.%'
    `);
  }

  // Suppression atomique : le DELETE ré-applique les mêmes filtres que la sélection,
  // pour ne pas supprimer un compte reconnecté (deletion_warned_at remis à NULL) entre
  // un SELECT et un DELETE séparés.
  async deleteInactiveUsers(grace: string): Promise<Array<{ _id: number }>> {
    return await this.pgConnection.query<{ _id: number }>(sql`
      DELETE FROM ${raw(this.table)}
      WHERE deletion_warned_at < now() - ${grace}::interval
        AND last_login_at < deletion_warned_at
        AND role NOT LIKE 'registry.%'
      RETURNING _id
    `);
  }
}
