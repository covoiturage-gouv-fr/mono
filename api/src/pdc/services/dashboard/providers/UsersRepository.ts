import { NotFoundException, provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { join, raw } from "@/lib/pg/sql.ts";
import { UserResult } from "@/pdc/services/dashboard/actions/users/UsersAction.ts";
import {
  CreateUserDataInterface,
  CreateUserResultInterface,
  DeleteUserParamsInterface,
  DeleteUserResultInterface,
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
  private readonly tableTerritory = "territory.territory_group";
  private readonly tableOperator = "operator.operators";

  constructor(private pgConnection: DenoPostgresConnection) {}

  async getUsers(
    params: UsersParamsInterface,
  ): Promise<UsersResultInterface> {
    const filters = [];
    if (params.id) {
      filters.push(sql`users._id = ${params.id}`);
    }
    if (params.operator_id) {
      filters.push(sql`users.operator_id = ${params.operator_id}`);
    }
    if (params.territory_id) {
      filters.push(sql`users.territory_id = ${params.territory_id}`);
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
      ${
      params.search
        ? sql`LEFT JOIN ${raw(this.tableTerritory)} tg ON tg._id = users.territory_id LEFT JOIN ${
          raw(this.tableOperator)
        } o ON o._id = users.operator_id`
        : sql``
    }
      WHERE ${join(filters, " AND ")}
      ORDER BY users._id
      LIMIT ${limit} OFFSET ${offset}
    `;
    const rows = await this.pgConnection.query<UserResult>(query);
    // Calcul du nombre total d'éléments
    const countQuery = sql`
      SELECT COUNT(*) as total
      FROM ${raw(this.table)} AS users
      ${params.search ? sql`LEFT JOIN ${raw(this.tableTerritory)} tg ON tg._id = users.territory_id` : sql``}
      LEFT JOIN ${raw(this.tableOperator)} o ON o._id = users.operator_id
      WHERE ${join(filters, " AND ")}
    `;
    const countResponse = await this.pgConnection.query<{ total: string }>(countQuery);
    return {
      meta: {
        total: parseInt(countResponse[0].total, 10),
        page: page,
        totalPages: Math.ceil(parseInt(countResponse[0].total, 10) / limit),
      },
      data: rows,
    };
  }

  async createUser(data: CreateUserDataInterface): Promise<CreateUserResultInterface> {
    const query = sql`
      INSERT INTO ${raw(this.table)} (
        firstname, lastname, email, role, operator_id, territory_id
      ) VALUES (
        ${data.firstname}, ${data.lastname}, ${data.email}, ${data.role}, ${data.operator_id}, ${data.territory_id}
      )
      RETURNING
        _id, created_at, firstname, lastname, email, role, operator_id, territory_id
    `;
    const rows = await this.pgConnection.query(query);
    if (rows.length !== 1) {
      throw new Error(`Unable to create user ${data}`);
    }
    return {
      success: true,
      message: `user ${JSON.stringify(rows[0])} created`,
    };
  }

  async deleteUser(params: DeleteUserParamsInterface): Promise<DeleteUserResultInterface> {
    const query = sql`
      DELETE ${raw(this.table)}
      WHERE _id = ${params.id}
      RETURNING _id
    `;
    const rows = await this.pgConnection.query(query);
    if (rows.length !== 1) {
      throw new NotFoundException(`user not found: (${params.id})`);
    }
    return { success: true, message: `user ${params.id} deleted` };
  }

  async updateUser(data: UpdateUserDataInterface): Promise<UpdateUserResultInterface> {
    const query = sql`
      UPDATE ${raw(this.table)}
      SET 
        firstname = ${data.firstname},
        lastname = ${data.lastname},
        email = ${data.email},
        role = ${data.role},
        operator_id = ${data.operator_id},
        territory_id = ${data.territory_id},
        updated_at = now()
      WHERE _id = ${data.id}
      RETURNING _id, updated_at, firstname, lastname, email, role, operator_id, territory_id
    `;
    const rows = await this.pgConnection.query<UpdateUserResultInterface>(query);
    if (rows.length !== 1) {
      throw new Error(`Unable to update user with id ${data.id}`);
    }
    return {
      success: true,
      message: `User ${JSON.stringify(rows[0])} updated`,
    };
  }
}
