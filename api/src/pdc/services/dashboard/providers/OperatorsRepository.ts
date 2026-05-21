import { NotFoundException, provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { join, raw } from "@/lib/pg/sql.ts";
import { OperatorResult } from "@/pdc/services/dashboard/actions/operators/OperatorsAction.ts";
import {
  CreateOperatorDataInterface,
  CreateOperatorResultInterface,
  DeleteOperatorParamsInterface,
  DeleteOperatorResultInterface,
  OperatorsParamsInterface,
  OperatorsRepositoryInterface,
  OperatorsRepositoryInterfaceResolver,
  OperatorsResultInterface,
  UpdateOperatorDataInterface,
  UpdateOperatorResultInterface,
} from "../interfaces/OperatorsRepositoryInterface.ts";

@provider({
  identifier: OperatorsRepositoryInterfaceResolver,
})
export class OperatorsRepository implements OperatorsRepositoryInterface {
  private readonly table = "operator.operators";

  constructor(private pgConnection: DenoPostgresConnection) {}

  async getOperators(
    params: OperatorsParamsInterface,
  ): Promise<OperatorsResultInterface> {
    const filters = [sql`deleted_at is null`];
    if (params.id) {
      filters.push(sql`_id=${params.id}`);
    }
    if (params.search) {
      filters.push(sql`(name ILIKE ${`%${params.search}%`} OR SIRET ILIKE ${`%${params.search}%`})`);
    }
    const limit = params.limit || 100;
    const page = params.page || 1;
    const offset = (page - 1) * limit;
    const query = sql`
      SELECT 
        _id as id,
        name,
        siret
      FROM ${raw(this.table)}
      WHERE ${join(filters, " AND ")}
      ORDER BY _id
      LIMIT ${limit} OFFSET ${offset}
    `;
    const rows = await this.pgConnection.query<OperatorResult>(query);
    // Calcul du nombre total d'éléments
    const countQuery = sql`
      SELECT COUNT(*) as total
      FROM ${raw(this.table)}
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

  async getOperatorById(id: number): Promise<OperatorResult | null> {
    const query = sql`
      SELECT
        _id as id,
        name,
        siret
      FROM ${raw(this.table)}
      WHERE _id = ${id} AND deleted_at IS NULL
    `;
    const rows = await this.pgConnection.query<OperatorResult>(query);
    return rows.length > 0 ? rows[0] : null;
  }

  async createOperator(
    data: CreateOperatorDataInterface,
  ): Promise<CreateOperatorResultInterface> {
    const query = sql`
      INSERT INTO ${raw(this.table)} (
        name, siret
      ) VALUES (
        ${data.name}, ${data.siret}
      )
      RETURNING _id as id, created_at, name, siret
    `;
    const rows = await this.pgConnection.query(query);
    if (rows.length !== 1) {
      throw new Error(`Unable to create operator ${data}`);
    }
    return {
      success: true,
      message: `Operator ${JSON.stringify(rows[0])} created`,
    };
  }

  async deleteOperator(
    params: DeleteOperatorParamsInterface,
  ): Promise<DeleteOperatorResultInterface> {
    const query = sql`
      UPDATE ${raw(this.table)}
      SET deleted_at = NOW()
      WHERE _id = ${params.id}
      RETURNING _id
    `;
    const rows = await this.pgConnection.query(query);
    if (rows.length !== 1) {
      throw new NotFoundException(`operator not found: (${params.id})`);
    }
    return { success: true, message: `Operator ${params.id} deleted` };
  }

  async updateOperator(
    data: UpdateOperatorDataInterface,
  ): Promise<UpdateOperatorResultInterface> {
    const query = sql`
      UPDATE ${raw(this.table)}
      SET 
        name = ${data.name},
        updated_at = now(),
        siret = ${data.siret}
      WHERE _id = ${data.id}
      RETURNING _id, updated_at, name, siret
    `;
    const rows = await this.pgConnection.query(query);
    if (rows.length !== 1) {
      throw new Error(`Unable to update operator with id ${data.id}`);
    }
    return {
      success: true,
      message: `Operator ${JSON.stringify(rows[0])} updated`,
    };
  }
}
