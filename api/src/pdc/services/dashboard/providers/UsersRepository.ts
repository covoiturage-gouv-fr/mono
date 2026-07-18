import { NotFoundException, provider } from "@/ilos/common/index.ts";
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
      ? sql`LEFT JOIN ${raw(this.tableTerritory)} tg ON tg._id = users.territory_id LEFT JOIN ${
        raw(this.tableOperator)
      } o ON o._id = users.operator_id`
      : sql``;
    // GROUP BY users._id (PK) : un user multi-territoires n'apparaît qu'une fois.
    const query = sql`
      SELECT
        users._id as id,
        users.firstname,
        users.lastname,
        users.email,
        users.operator_id,
        users.territory_id,
        users.role
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
    // Dual-write (expand) : on écrit encore les colonnes dépréciées operator_id/territory_id + login_siren.
    const query = sql`
      INSERT INTO ${raw(this.table)} (
        firstname, lastname, email, role, operator_id, territory_id, login_siren
      ) VALUES (
        ${data.firstname}, ${data.lastname}, ${data.email}, ${data.role}, ${data.operator_id}, ${data.territory_id}, ${
      data.login_siren ?? null
    }
      )
      RETURNING
        _id, created_at, firstname, lastname, email, role, operator_id, territory_id
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

  // Reconcilie user_scopes avec le formulaire (défaut + territoires additionnels), via UserScopeRepository.
  private async seedScopes(
    userId: number,
    data: { operator_id?: number | null; territory_id?: number | null; scopes?: number[] },
  ): Promise<void> {
    // Reset idempotent (no-op à la création, resynchronise à l'update).
    await this.pgConnection.query(sql`
      DELETE FROM ${raw(this.tableScopes)} WHERE user_id = ${userId}
    `);

    if (data.operator_id) {
      await this.userScopeRepository.setOperator(userId, data.operator_id);
      return;
    }

    const territories: number[] = [];
    if (data.territory_id) territories.push(data.territory_id);
    for (const t of data.scopes ?? []) {
      if (!territories.includes(t)) territories.push(t);
    }
    for (let i = 0; i < territories.length; i++) {
      await this.userScopeRepository.addTerritory(userId, territories[i], i === 0);
    }
  }

  async deleteUser(
    params: DeleteUserParamsInterface & { operator_id?: number; territory_id?: number },
  ): Promise<DeleteUserResultInterface> {
    const filters = [sql`_id = ${params.id}`];
    if (params.operator_id) {
      filters.push(sql`operator_id = ${params.operator_id}`);
    }
    if (params.territory_id) {
      filters.push(sql`territory_id = ${params.territory_id}`);
    }

    const query = sql`
      DELETE FROM ${raw(this.table)}
      WHERE ${join(filters, " AND ")}
      RETURNING _id
    `;
    const rows = await this.pgConnection.query(query);
    if (rows.length !== 1) {
      throw new NotFoundException();
    }
    return { success: true, message: `user ${params.id} deleted` };
  }

  async updateUser(data: UpdateUserDataInterface): Promise<UpdateUserResultInterface> {
    // Dual-write (expand) : colonnes dépréciées + login_siren, puis resynchro du pivot.
    const query = sql`
      UPDATE ${raw(this.table)}
      SET
        firstname = ${data.firstname},
        lastname = ${data.lastname},
        email = ${data.email},
        role = ${data.role},
        operator_id = ${data.operator_id},
        territory_id = ${data.territory_id},
        login_siren = ${data.login_siren ?? null},
        updated_at = now()
      WHERE _id = ${data.id}
      RETURNING _id, updated_at, firstname, lastname, email, role, operator_id, territory_id
    `;
    const rows = await this.pgConnection.query<UpdateUserResultInterface>(query);
    if (rows.length !== 1) {
      throw new Error(`Unable to update user with id ${data.id}`);
    }
    await this.seedScopes(data.id, data);
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
