import { NotFoundException, provider } from "@/ilos/common/index.ts";
import { LegacyPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import { logger } from "@/lib/logger/index.ts";
import { PolicyStatusEnum } from "../contracts/common/interfaces/PolicyInterface.ts";
import { toISOString } from "../helpers/index.ts";
import { PolicyRepositoryProviderInterfaceResolver, SerializedPolicyInterface } from "../interfaces/index.ts";

@provider({
  identifier: PolicyRepositoryProviderInterfaceResolver,
})
export class PolicyRepositoryProvider implements PolicyRepositoryProviderInterfaceResolver {
  public readonly table = "policy.policies";
  public readonly getTerritorySelectorFn = "territory.get_selector_by_territory_id";

  constructor(protected connection: LegacyPostgresConnection) {}

  async find(id: number, territoryId?: number): Promise<SerializedPolicyInterface | undefined> {
    const query = {
      text: `
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
        FROM ${this.table} as pp,
        LATERAL (
          SELECT * FROM ${this.getTerritorySelectorFn}(ARRAY[pp.territory_id])
        ) as sel
        WHERE pp._id = $1
        AND pp.deleted_at IS NULL
        ${!!territoryId ? "AND pp.territory_id = $2" : ""}
        LIMIT 1
      `,
      values: [id, ...(territoryId ? [territoryId] : [])],
    };

    const result = await this.connection.getClient().query(query);

    if (result.rowCount === 0) {
      return undefined;
    }

    return result.rows[0];
  }

  async create(data: Omit<SerializedPolicyInterface, "_id">): Promise<SerializedPolicyInterface> {
    const query = {
      text: `
        INSERT INTO ${this.table} (
          territory_id,
          start_date,
          end_date,
          name,
          status,
          handler
        ) VALUES (
          $1,
          $2,
          $3,
          $4,
          $5,
          $6
        )
        RETURNING _id
      `,
      values: [
        data.territory_id,
        data.start_date,
        data.end_date,
        data.name,
        data.status,
        data.handler,
      ],
    };

    const result = await this.connection.getClient().query(query);
    if (result.rowCount !== 1) {
      throw new Error(`Unable to create campaign (${JSON.stringify(data)})`);
    }

    return await this.find(result.rows[0]._id);
  }

  async patch(data: SerializedPolicyInterface): Promise<SerializedPolicyInterface> {
    const query = {
      text: `
      UPDATE ${this.table}
        SET
           name = $2,
           start_date = $3,
           end_date = $4,
           handler = $5,
           status = $6
        WHERE _id = $1
        AND deleted_at IS NULL
        RETURNING _id
      `,
      values: [
        data._id,
        data.name,
        data.start_date,
        data.end_date,
        data.handler,
        data.status,
      ],
    };

    const result = await this.connection.getClient().query(query);
    if (result.rowCount !== 1) {
      throw new NotFoundException(`campaign not found (${data._id})`);
    }

    return await this.find(result.rows[0]._id);
  }

  async delete(id: number): Promise<void> {
    const query = {
      text: `
      UPDATE ${this.table}
        SET deleted_at = NOW()
        WHERE _id = $1 AND deleted_at IS NULL
      `,
      values: [id],
    };

    const result = await this.connection.getClient().query(query);

    if (result.rowCount !== 1) {
      throw new NotFoundException(`Campaign not found (${id})`);
    }

    return;
  }

  async updateDescriptiveSheetUrl(id: number, descriptiveSheetUrl: string | null): Promise<void>{
    const query = {
      text: `
      UPDATE ${this.table}
        SET descriptive_sheet_url = $2
        WHERE _id = $1
        AND deleted_at IS NULL
        RETURNING _id
      `,
      values: [id, descriptiveSheetUrl],
    };

    const result = await this.connection.getClient().query(query);

    if (result.rowCount !== 1) {
      throw new NotFoundException(`Campaign not found (${id})`);
    }
  }

  async listApplicablePoliciesId(): Promise<number[]> {
    const results = await this.connection.getClient().query({
      text: "SELECT _id FROM policy.policies WHERE status = $1",
      values: [PolicyStatusEnum.ACTIVE],
    });

    return results.rows.map((r: { _id: number }) => r._id);
  }

  async findWhere(search: {
    _id?: number;
    territory_id?: number | null | number[];
    status?: PolicyStatusEnum;
    datetime?: Date;
    ends_in_the_future?: boolean;
  }): Promise<SerializedPolicyInterface[]> {
    const values = [];
    const whereClauses = ["deleted_at IS NULL and handler is not null"];
    for (const key of Reflect.ownKeys(search)) {
      switch (key) {
        case "_id":
          values.push(search[key]);
          whereClauses.push(`pp._id = $${values.length}`);
          break;
        case "status":
          values.push(search[key]);
          whereClauses.push(`pp.status = $${values.length}`);
          break;
        case "territory_id": {
          const tid = search[key];
          if (tid === null) {
            whereClauses.push("pp.territory_id IS NULL");
          } else if (Array.isArray(tid)) {
            values.push(tid);
            whereClauses.push(
              `pp.territory_id = ANY($${values.length}::int[])`,
            );
          } else {
            values.push(tid);
            whereClauses.push(`pp.territory_id = $${values.length}::int`);
          }
          break;
        }
        case "datetime":
          values.push(search[key]);
          whereClauses.push(
            `pp.start_date <= $${values.length}::timestamp AND pp.end_date >= $${values.length}::timestamp`,
          );
          break;
        case "ends_in_the_future":
          whereClauses.push(`pp.end_date ${search[key] ? ">" : "<"} NOW()`);
          break;
        default:
          break;
      }
    }
    const query = {
      values,
      text: `
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
        FROM ${this.table} as pp,
        LATERAL (
          SELECT * FROM ${this.getTerritorySelectorFn}(ARRAY[pp.territory_id])
        ) as sel
        WHERE ${whereClauses.join(" AND ")}
      `,
    };

    const result = await this.connection.getClient().query(query);
    return result.rows;
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
    const query = {
      text: `
        SELECT pi.operator_id
        FROM policy.incentives pi
        JOIN carpool_v2.carpools cc ON cc.operator_id = pi.operator_id AND cc.operator_journey_id = pi.operator_journey_id
        JOIN policy.policies pp ON pp._id = $1
        WHERE
              cc.start_datetime >= pp.start_date
          AND cc.start_datetime <  pp.end_date
          AND pi.policy_id = $1
          AND pi.state = 'regular'
        GROUP BY cc.operator_id
        ORDER BY cc.operator_id
      `,
      values: [policy_id],
    };

    const result = await this.connection.getClient().query(query);
    return result.rowCount ? result.rows.map((o: { operator_id: number }) => o.operator_id) : [];
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
    const res = await this.connection.getClient().query<{ _id: number; datetime: Date }>({
      text: `
          SELECT _id, datetime FROM policy.policy_metas
          WHERE policy_id = $1 AND key = $2
          ORDER BY datetime DESC
          LIMIT 1
        `,
      values: [campaign_id, key_name],
    });

    if (!res.rowCount) {
      logger.warn(`${pf} ${key_name} key not found for campaign ${campaign_id}`);
      return;
    }

    const { _id: key_id, datetime } = res.rows[0];

    // compute incentive_sum
    const resSum = await this.connection.getClient().query<{ incentive_sum: number }>({
      text: `
          WITH latest_incentive AS (
            SELECT MAX(datetime)
            FROM policy.incentives
            WHERE policy_id = $1
              AND status = 'validated'
          )
          SELECT COALESCE(SUM(amount)::int, 0) AS incentive_sum
          FROM policy.incentives
          WHERE policy_id = $1
            AND datetime <= (SELECT max FROM latest_incentive)
            AND status = 'validated'
          `,
      values: [campaign_id],
    });

    if (!resSum.rowCount) {
      logger.warn(`${pf} Could not calculate incentive sum for campaign ${campaign_id}`);
      return;
    }

    const { incentive_sum } = resSum.rows[0];

    // update max_amount_restriction.global.campaign.global
    logger.info(
      `${pf} Setting policy_meta (${key_id}) ` +
        `to ${incentive_sum} ` +
        `at ${toISOString(datetime)} ` +
        `for campaign ${campaign_id}`,
    );

    await this.connection.getClient().query({
      text: `UPDATE policy.policy_metas SET value = $2 WHERE _id = $1`,
      values: [key_id, incentive_sum],
    });

    // update incentive_sum in the policy
    logger.info(`${pf} Set incentive_sum ${incentive_sum} in policy ${campaign_id}`);
    await this.connection.getClient().query({
      text: `UPDATE policy.policies SET incentive_sum = LEAST(max_amount, $2) WHERE _id = $1`,
      values: [campaign_id, incentive_sum],
    });
  }

  /**
   * Mass update campaign status
   *
   * For each campaign, check if it is still active and update its status
   */
  async updateAllCampaignStatuses(): Promise<void> {
    await this.connection.getClient().query({
      text: `
        UPDATE ${this.table} SET status = $1
        WHERE end_date < CURRENT_TIMESTAMP AND status = $2
      `,
      values: [PolicyStatusEnum.FINISHED, PolicyStatusEnum.ACTIVE],
    });
  }
}
