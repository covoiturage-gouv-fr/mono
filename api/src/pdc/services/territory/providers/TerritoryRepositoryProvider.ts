import { KernelInterfaceResolver, NotFoundException, provider } from "@/ilos/common/index.ts";
import { LegacyPostgresConnection, PoolClient } from "@/ilos/connection-postgres/index.ts";
import { TerritoryParams } from "@/pdc/services/territory/actions/group/ListTerritoryActionV2.ts";
import { TerritorySelectorsInterface } from "@/pdc/services/territory/contracts/common/interfaces/TerritoryCodeInterface.ts";
import {
  CreateParamsInterface,
  CreateResultInterface,
  FindParamsInterface,
  FindResultInterface,
  ListResultInterface,
  PatchContactsParamsInterface,
  PatchContactsResultInterface,
  TerritoryRepositoryProviderInterface,
  TerritoryRepositoryProviderInterfaceResolver,
  UpdateParamsInterface,
  UpdateResultInterface,
} from "../interfaces/TerritoryRepositoryProviderInterface.ts";

@provider({
  identifier: TerritoryRepositoryProviderInterfaceResolver,
})
export class TerritoryRepositoryProvider implements TerritoryRepositoryProviderInterface {
  public readonly table = "territory.territory_group";
  public readonly relationTable = "territory.territory_group_selector";
  public readonly GROUP_DEFAULT_SHORT_NAME = "";

  constructor(
    protected connection: LegacyPostgresConnection,
    protected kernel: KernelInterfaceResolver,
  ) {}

  async find(params: FindParamsInterface): Promise<FindResultInterface> {
    const result = await this.connection.getClient().query<any>({
      text: `
        WITH selector_raw AS (
          SELECT
            territory_group_id,
            selector_type,
            ARRAY_AGG(selector_value) as selector_value
          FROM ${this.relationTable}
          WHERE territory_group_id = $1
          GROUP BY territory_group_id, selector_type
        ),
        selector AS (
          SELECT
            territory_group_id,
            JSON_OBJECT_AGG(
              selector_type,
              selector_value
            ) as selector
          FROM selector_raw
          GROUP BY territory_group_id
        ) 
        SELECT
          tg._id,
          tg.company_id,
          tg.created_at,
          tg.updated_at,
          tg.name,
          tg.shortname,
          tg.contacts,
          tg.address,
          tgs.selector
        FROM ${this.table} AS tg
        LEFT JOIN selector AS tgs
          ON tgs.territory_group_id = tg._id
        WHERE 
          tg._id = $1 AND
          tg.deleted_at IS NULL
      `,
      values: [params._id],
    });

    if (result.rowCount === 0) {
      throw new NotFoundException();
    }

    return result.rows[0];
  }

  async list(params: TerritoryParams): Promise<ListResultInterface> {
    const { search, offset, limit, operator_id, policy } = { limit: 100, offset: 0, ...params };
    const values = [];
    const whereClauses: string[] = [];

    if (search) {
      values.push(`%${search.toLowerCase()}%`);
      whereClauses.push(`(LOWER(tg.name) LIKE $${values.length} or cc.siret LIKE $${values.length})`);
    }

    const client = this.connection.getClient();
    if (policy === true) {
      const policyQuery = {
        text: `
        SELECT DISTINCT(territory_id) as _id
        FROM policy.policies
        WHERE deleted_at IS NULL AND status = 'active'`,
      };
      const policiesResults = await client.query<any>(policyQuery);

      if (policiesResults.rowCount === 0) {
        return {
          data: [],
          meta: { pagination: { offset, limit, total: 0 } },
        };
      }
      values.push(policiesResults.rows.map((r: { _id: number }) => r._id));
      whereClauses.push(
        `tg._id = ANY ($1::int[])`,
      );
    }
    if (operator_id) {
      const operatorQuery = {
        text: `
        WITH operator_incentives AS (SELECT DISTINCT(policy_id) FROM policy.incentives WHERE operator_id = $1)
        SELECT DISTINCT(tg._id) FROM policy.policies pp 
        JOIN territory.territory_group tg ON tg._id = pp.territory_id
        WHERE pp._id IN (SELECT * FROM operator_incentives)`,
        values: [operator_id],
      };

      const territoriesForOperatorResults = await client.query<any>(operatorQuery);

      if (territoriesForOperatorResults.rowCount === 0) {
        return {
          data: [],
          meta: { pagination: { offset, limit, total: 0 } },
        };
      }

      values.push(territoriesForOperatorResults.rows.map((r: { _id: number }) => r._id));
      whereClauses.push(
        `tg._id = ANY ($1::int[])`,
      );
    }
    const countQuery = `SELECT count(*) as territory_count from ${this.table} as tg
      ${search ? `JOIN company.companies cc ON cc._id = tg.company_id` : ``}
      ${whereClauses.length ? ` WHERE ${whereClauses.join(" AND ")}` : ""}`;

    const total = parseFloat(
      (
        await client.query<any>({
          text: countQuery,
          values,
        })
      ).rows[0].territory_count,
    );

    values.push(limit);
    values.push(offset);
    const query = {
      text: `
        SELECT tg._id, tg.name, cc.siret FROM ${this.table} as tg
        JOIN company.companies cc ON cc._id = tg.company_id
        WHERE tg.deleted_at IS NULL ${whereClauses.length ? `AND ${whereClauses.join(" AND ")}` : ""}
        ORDER BY tg.name ASC
        LIMIT $${whereClauses.length + 1}
        OFFSET $${whereClauses.length + 2}
      `,
      values,
    };
    const result = await client.query<any>(query);
    return {
      data: result.rows,
      meta: { pagination: { offset, limit, total } },
    };
  }

  async create(data: CreateParamsInterface): Promise<CreateResultInterface> {
    const connection = await this.connection.getClient().connect();
    await connection.query<any>("BEGIN");
    try {
      const fields = ["name", "shortname", "contacts", "address", "company_id"];

      const values: any[] = [
        data.name,
        this.GROUP_DEFAULT_SHORT_NAME,
        data.contacts,
        data.address,
        data.company_id,
      ];

      const query = {
        text: `
          INSERT INTO ${this.table} (${fields.join(",")})
          VALUES (${fields.map((data, ind) => `$${ind + 1}`).join(",")})
          RETURNING *
        `,
        values,
      };

      const result = await connection.query<any>(query);
      if (result.rowCount !== 1) {
        throw new Error(`Unable to create territory (${JSON.stringify(data)})`);
      }

      const resultData = result.rows[0];

      if (data.selector) {
        await this.syncSelector(connection, resultData._id, data.selector);
      }

      await connection.query<any>("COMMIT");
      return { ...resultData, selector: data.selector };
    } catch (e) {
      await connection.query<any>("ROLLBACK");
      throw e;
    } finally {
      connection.release();
    }
  }

  protected async syncSelector(
    connection: PoolClient,
    groupId: number,
    selector: TerritorySelectorsInterface,
  ): Promise<void> {
    const values: [number[], string[], string[]] = Object.keys(selector)
      .map((type) =>
        selector[type].map((
          value: string | number,
        ) => [groupId, type, value.toString()])
      )
      .reduce((arr, v) => [...arr, ...v], [])
      .reduce(
        (arr, v) => {
          arr[0].push(v[0]);
          arr[1].push(v[1]);
          arr[2].push(v[2]);
          return arr;
        },
        [[], [], []],
      );
    await connection.query<any>({
      text: `
        DELETE FROM ${this.relationTable}
        WHERE territory_group_id = $1
      `,
      values: [groupId],
    });

    const query = {
      text: `
        INSERT INTO ${this.relationTable} (
          territory_group_id,
          selector_type,
          selector_value
        ) 
        SELECT * FROM UNNEST($1::int[], $2::varchar[], $3::varchar[])`,
      values,
    };

    await connection.query<any>(query);
    return;
  }

  async delete(id: number): Promise<void> {
    const query = {
      text: `
      UPDATE ${this.table}
        SET deleted_at = NOW()
        WHERE _id = $1
      `,
      values: [id],
    };

    const result = await this.connection.getClient().query<any>(query);

    if (result.rowCount !== 1) {
      throw new NotFoundException(`territory not found (${id})`);
    }

    return;
  }

  async update(data: UpdateParamsInterface): Promise<UpdateResultInterface> {
    const connection = await this.connection.getClient().connect();
    await connection.query<any>("BEGIN");
    try {
      const fields = ["name", "shortname", "contacts", "address", "company_id"];

      const values: any[] = [
        data.name,
        "",
        data.contacts,
        data.address,
        data.company_id,
      ];

      const query = {
        text: `
          UPDATE ${this.table}
          SET ${fields.map((val, ind) => `${val} = $${ind + 1}`).join(",")}
          WHERE _id = ${data._id}
          RETURNING *
        `,
        values,
      };

      const result = await connection.query<any>(query);

      if (result.rowCount !== 1) {
        throw new Error(`Unable to create territory (${JSON.stringify(data)})`);
      }

      const resultData = result.rows[0];

      if (data.selector) {
        await this.syncSelector(connection, resultData._id, data.selector);
      }

      await connection.query<any>("COMMIT");
      return { ...resultData, selector: data.selector };
    } catch (e) {
      await connection.query<any>("ROLLBACK");
      throw e;
    } finally {
      connection.release();
    }
  }

  async patchContacts(
    params: PatchContactsParamsInterface,
  ): Promise<PatchContactsResultInterface> {
    const query = {
      text: `UPDATE ${this.table} set contacts = $2 WHERE _id = $1`,
      values: [params._id, JSON.stringify(params.patch)],
    };

    const client = this.connection.getClient();
    await client.query<any>(query);

    const modifiedTerritoryRes = await client.query<any>({
      text: `SELECT * FROM ${this.table} WHERE _id = $1`,
      values: [params._id],
    });

    return modifiedTerritoryRes.rows[0];
  }

  async getRelationCodes(
    params: { _id: number },
  ): Promise<TerritorySelectorsInterface> {
    const query = {
      text: `
        WITH selector_raw AS (
          SELECT
            territory_group_id,
            selector_type,
            ARRAY_AGG(selector_value) as selector_value
          FROM ${this.relationTable}
          WHERE territory_group_id = $1
          GROUP BY territory_group_id, selector_type
        )
        SELECT
          territory_group_id,
          JSON_OBJECT_AGG(
            selector_type,
            selector_value
          ) as selector
        FROM selector_raw
        GROUP BY territory_group_id
      `,
      values: [params._id],
    };

    const result = await this.connection.getClient().query<any>(query);
    return result.rows[0].selector;
  }

  async getRelationCodesCom(
    params: { _id: number },
  ): Promise<TerritorySelectorsInterface> {
    const query = {
      text: `
      SELECT
        ARRAY_AGG(com) AS com
      FROM territory.get_com_by_territory_id(
        $1::int,
        (select * from geo.get_latest_millesime())
      )
      `,
      values: [params._id],
    };

    const result = await this.connection.getClient().query<any>(query);
    return result.rows[0];
  }
}
