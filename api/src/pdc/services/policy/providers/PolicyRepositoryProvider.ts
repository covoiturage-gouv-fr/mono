import { NotFoundException, provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import { logger } from "@/lib/logger/index.ts";
import sql, { empty, join, raw, Sql } from "@/lib/pg/sql.ts";
import { PolicyStatusEnum } from "../contracts/common/interfaces/PolicyInterface.ts";
import { toISOString } from "../helpers/index.ts";
import { PolicyRepositoryProviderInterfaceResolver, SerializedPolicyInterface } from "../interfaces/index.ts";

@provider({
  identifier: PolicyRepositoryProviderInterfaceResolver,
})
export class PolicyRepositoryProvider implements PolicyRepositoryProviderInterfaceResolver {
  public readonly table = "policy.policies";
  public readonly getTerritorySelectorFn = "territory.get_selector_by_territory_id";
  private readonly tableTerritory = "territory.territory_group";

  constructor(protected pgConnection: DenoPostgresConnection) {}

  async find(id: number, territoryId?: number): Promise<SerializedPolicyInterface | undefined> {
    const territoryFilter = territoryId !== undefined
      ? sql`AND pp.territory_id = ${territoryId}`
      : empty;

    const rows = await this.pgConnection.query<SerializedPolicyInterface>(sql`
      SELECT
        pp._id,
        sel.selector as territory_selector,
        pp.name,
        pp.descriptive_sheet_url,
        pp.start_date,
        pp.end_date,
        pp.tz,
        pp.handler,
        pp.status,
        pp.territory_id,
        pp.incentive_sum,
        pp.max_amount
      FROM ${raw(this.table)} as pp,
      LATERAL (
        SELECT * FROM ${raw(this.getTerritorySelectorFn)}(ARRAY[pp.territory_id])
      ) as sel
      WHERE pp._id = ${id}
      AND pp.deleted_at IS NULL
      ${territoryFilter}
      LIMIT 1
    `);

    return rows[0];
  }

  async create(data: Omit<SerializedPolicyInterface, "_id">): Promise<SerializedPolicyInterface> {
    const rows = await this.pgConnection.query<{ _id: number }>(sql`
      INSERT INTO ${raw(this.table)} (
        territory_id,
        start_date,
        end_date,
        name,
        status,
        handler
      ) VALUES (
        ${data.territory_id},
        ${data.start_date},
        ${data.end_date},
        ${data.name},
        ${data.status},
        ${data.handler}
      )
      RETURNING _id
    `);
    if (rows.length !== 1) {
      throw new Error(`Unable to create campaign (${JSON.stringify(data)})`);
    }

    return await this.find(rows[0]._id);
  }

  async patch(data: SerializedPolicyInterface): Promise<SerializedPolicyInterface> {
    const rows = await this.pgConnection.query<{ _id: number }>(sql`
      UPDATE ${raw(this.table)}
        SET
           name = ${data.name},
           start_date = ${data.start_date},
           end_date = ${data.end_date},
           handler = ${data.handler},
           status = ${data.status}
        WHERE _id = ${data._id}
        AND deleted_at IS NULL
        RETURNING _id
    `);
    if (rows.length !== 1) {
      throw new NotFoundException(`campaign not found (${data._id})`);
    }

    return await this.find(rows[0]._id);
  }

  async delete(id: number): Promise<void> {
    const rows = await this.pgConnection.query<{ _id: number }>(sql`
      UPDATE ${raw(this.table)}
        SET deleted_at = NOW()
        WHERE _id = ${id} AND deleted_at IS NULL
        RETURNING _id
    `);

    if (rows.length !== 1) {
      throw new NotFoundException(`Campaign not found (${id})`);
    }
  }

  async updateDescriptiveSheetUrl(id: number, descriptiveSheetUrl: string | null): Promise<void> {
    const rows = await this.pgConnection.query<{ _id: number }>(sql`
      UPDATE ${raw(this.table)}
        SET descriptive_sheet_url = ${descriptiveSheetUrl}
        WHERE _id = ${id}
        AND deleted_at IS NULL
        RETURNING _id
    `);

    if (rows.length !== 1) {
      throw new NotFoundException(`Campaign not found (${id})`);
    }
  }

  async listApplicablePoliciesId(): Promise<number[]> {
    const rows = await this.pgConnection.query<{ _id: number }>(sql`
      SELECT _id FROM policy.policies WHERE status = ${PolicyStatusEnum.ACTIVE}
    `);

    return rows.map((r) => r._id);
  }

  async findWhere(search: {
    _id?: number;
    territory_id?: number | null | number[];
    status?: PolicyStatusEnum;
    datetime?: Date;
    ends_in_the_future?: boolean;
    search?: string;
  }): Promise<SerializedPolicyInterface[]> {
    const filters: Sql[] = [sql`pp.deleted_at IS NULL and handler is not null`];
    for (const key of Reflect.ownKeys(search)) {
      switch (key) {
        case "_id":
          filters.push(sql`pp._id = ${search[key]}`);
          break;
        case "status":
          filters.push(sql`pp.status = ${search[key]}`);
          break;
        case "territory_id": {
          const tid = search[key];
          if (tid === null) {
            filters.push(sql`pp.territory_id IS NULL`);
          } else if (Array.isArray(tid)) {
            filters.push(sql`pp.territory_id = ANY(${tid}::int[])`);
          } else {
            filters.push(sql`pp.territory_id = ${tid}::int`);
          }
          break;
        }
        case "datetime": {
          const dt = search[key];
          filters.push(sql`pp.start_date <= ${dt}::timestamp AND pp.end_date >= ${dt}::timestamp`);
          break;
        }
        case "ends_in_the_future":
          filters.push(search[key] ? sql`pp.end_date > NOW()` : sql`pp.end_date < NOW()`);
          break;
        case "search": {
          const pattern = `%${search[key]}%`;
          filters.push(sql`(pp.name ILIKE ${pattern} OR tg.name ILIKE ${pattern})`);
          break;
        }
        default:
          break;
      }
    }

    return await this.pgConnection.query<SerializedPolicyInterface>(sql`
      SELECT
        pp._id,
        sel.selector as territory_selector,
        pp.name,
        pp.descriptive_sheet_url,
        pp.start_date,
        pp.end_date,
        pp.handler,
        pp.status,
        pp.territory_id,
        pp.incentive_sum,
        pp.max_amount
      FROM ${raw(this.table)} as pp
      LEFT JOIN ${raw(this.tableTerritory)} as tg ON tg._id = pp.territory_id
      CROSS JOIN LATERAL (
        SELECT * FROM ${raw(this.getTerritorySelectorFn)}(ARRAY[pp.territory_id])
      ) as sel
      WHERE ${join(filters, " AND ")}
    `);
  }

  /**
   * List all operator_id having valid trips for a given campaign.
   *
   * This approach, although slower, is based on data on the contrary
   * to instantiating the campaign engine class and getting the list
   * of operators from the params() method. The latter does not keep
   * history of in-campaign deleted operators.
   *
   * TODO de-duplicate with api/services/trip/src/providers/TripRepositoryProvider.ts:529
   */
  async activeOperators(policy_id: number): Promise<number[]> {
    const rows = await this.pgConnection.query<{ operator_id: number }>(sql`
      SELECT pi.operator_id
      FROM policy.incentives pi
      JOIN carpool_v2.carpools cc ON cc.operator_id = pi.operator_id AND cc.operator_journey_id = pi.operator_journey_id
      JOIN policy.policies pp ON pp._id = ${policy_id}
      WHERE
            cc.start_datetime >= pp.start_date
        AND cc.start_datetime <  pp.end_date
        AND pi.policy_id = ${policy_id}
        AND pi.state = 'regular'
      GROUP BY cc.operator_id
      ORDER BY cc.operator_id
    `);
    return rows.map((o) => o.operator_id);
  }

  /**
   * Synchronise max_amount_restriction.global.campaign.global key
   * from policy.policy_metas table to the computed sum of all validated
   * incentives at the date of the last validated incentive.
   *
   * note: the datetime of the key does not reflect the datetime of the last
   *       validated incentive but the datetime of the first value used to
   *       create the key. #FIXME
   */
  async syncIncentiveSum(campaign_id: number): Promise<void> {
    const pf = "[campaign:syncincentivesum]";
    const key_name = "max_amount_restriction.global.campaign.global";

    // update the campaign with the sum of validated incentives
    // get the key
    const metaRows = await this.pgConnection.query<{ _id: number; datetime: Date }>(sql`
      SELECT _id, datetime FROM policy.policy_metas
      WHERE policy_id = ${campaign_id} AND key = ${key_name}
      ORDER BY datetime DESC
      LIMIT 1
    `);

    if (!metaRows.length) {
      logger.warn(`${pf} ${key_name} key not found for campaign ${campaign_id}`);
      return;
    }

    const { _id: key_id, datetime } = metaRows[0];

    // compute incentive_sum
    const sumRows = await this.pgConnection.query<{ incentive_sum: number }>(sql`
      WITH latest_incentive AS (
        SELECT MAX(datetime)
        FROM policy.incentives
        WHERE policy_id = ${campaign_id}
          AND status = 'validated'
      )
      SELECT COALESCE(SUM(amount)::int, 0) AS incentive_sum
      FROM policy.incentives
      WHERE policy_id = ${campaign_id}
        AND datetime <= (SELECT max FROM latest_incentive)
        AND status = 'validated'
    `);

    if (!sumRows.length) {
      logger.warn(`${pf} Could not calculate incentive sum for campaign ${campaign_id}`);
      return;
    }

    const { incentive_sum } = sumRows[0];

    // update max_amount_restriction.global.campaign.global
    logger.info(
      `${pf} Setting policy_meta (${key_id}) ` +
        `to ${incentive_sum} ` +
        `at ${toISOString(datetime)} ` +
        `for campaign ${campaign_id}`,
    );

    await this.pgConnection.query(sql`
      UPDATE policy.policy_metas SET value = ${incentive_sum} WHERE _id = ${key_id}
    `);

    // update incentive_sum in the policy
    logger.info(`${pf} Set incentive_sum ${incentive_sum} in policy ${campaign_id}`);
    await this.pgConnection.query(sql`
      UPDATE policy.policies SET incentive_sum = LEAST(max_amount, ${incentive_sum}) WHERE _id = ${campaign_id}
    `);
  }

  /**
   * Mass update campaign status
   *
   * For each campaign, check if it is still active and update its status
   */
  async updateAllCampaignStatuses(): Promise<void> {
    await this.pgConnection.query(sql`
      UPDATE ${raw(this.table)} SET status = ${PolicyStatusEnum.FINISHED}
      WHERE end_date < CURRENT_TIMESTAMP AND status = ${PolicyStatusEnum.ACTIVE}
    `);
  }
}
