import { provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import { logger } from "@/lib/logger/index.ts";
import sql, { empty, raw } from "@/lib/pg/sql.ts";
import {
  IncentiveRepositoryProviderInterfaceResolver,
  IncentiveStateEnum,
  IncentiveStatusEnum,
  SerializedIncentiveInterface,
  SerializedMetadataVariableDefinitionInterface,
} from "../interfaces/index.ts";

@provider({
  identifier: IncentiveRepositoryProviderInterfaceResolver,
})
export class IncentiveRepositoryProvider implements IncentiveRepositoryProviderInterfaceResolver {
  public readonly incentivesTable = "policy.incentives";
  public readonly carpoolTable = "carpool_v2.carpools";
  public readonly carpoolStatusTable = "carpool_v2.status";

  constructor(protected pgConnection: DenoPostgresConnection) {}

  async disableOnExcludedCarpool(from: Date, to: Date): Promise<void> {
    await this.pgConnection.query(sql`
      UPDATE ${raw(this.incentivesTable)} AS pi
      SET
        state = 'disabled'::policy.incentive_state_enum,
        status = 'error'::policy.incentive_status_enum
      FROM ${raw(this.carpoolTable)} AS cc
      JOIN ${raw(this.carpoolStatusTable)} AS cs
        ON cs.carpool_id = cc._id
      WHERE cc.start_datetime >= ${from}::timestamp
        AND cc.start_datetime <  ${to}::timestamp
        AND cc.operator_id = pi.operator_id
        AND cc.operator_journey_id = pi.operator_journey_id
        AND (
          cs.acquisition_status = 'canceled'
          OR cs.anomaly_status = 'failed'
          OR cs.fraud_status = 'failed'
        )
    `);
  }

  /**
   * Set the status of pending incentives to either Draft or Validated
   */
  async setStatus(from: Date, to: Date, hasFailed = false): Promise<void> {
    const nextStatus = hasFailed ? IncentiveStatusEnum.Draft : IncentiveStatusEnum.Validated;
    await this.pgConnection.query(sql`
      UPDATE ${raw(this.incentivesTable)}
        SET status = ${nextStatus}::policy.incentive_status_enum
      WHERE datetime >= ${from}::timestamp
        AND datetime <  ${to}::timestamp
        AND status = ${IncentiveStatusEnum.Pending}::policy.incentive_status_enum
    `);
  }

  async updateStatefulAmount(
    data: SerializedIncentiveInterface<number>[],
    status?: IncentiveStatusEnum,
  ): Promise<void> {
    const idSet: Set<string> = new Set();

    // get only last incentive for each carpool / policy
    const filteredData = data.reverse().filter((d) => {
      const key = `${d.policy_id}/${d.operator_id}/${d.operator_journey_id}`;
      if (idSet.has(key)) {
        return false;
      }
      idSet.add(key);
      return true;
    });

    // pick values for the given keys. Override status if defined
    const ids: number[] = [];
    const amounts: number[] = [];
    const statuses: IncentiveStatusEnum[] = [];
    for (const i of filteredData) {
      ids.push(i._id);
      amounts.push(i.statefulAmount);
      statuses.push(status ?? i.status);
    }

    await this.pgConnection.query(sql`
      WITH data AS (
        SELECT * FROM UNNEST (
          ${ids}::int[],
          ${amounts}::int[],
          ${statuses}::policy.incentive_status_enum[]
        ) as t(
          _id,
          amount,
          status
        )
      )
      UPDATE ${raw(this.incentivesTable)} as pi
      SET (
        amount,
        state,
        status
      ) = (
        data.amount,
        CASE WHEN data.amount = 0 THEN 'null'::policy.incentive_state_enum ELSE state END,
        data.status
      )
      FROM data
      WHERE
        data._id = pi._id
    `);
  }

  async *findDraftIncentive(
    to: Date,
    batchSize = 100,
    from?: Date,
  ): AsyncGenerator<SerializedIncentiveInterface<number>[], void, void> {
    const fromFilter = from ? sql`AND datetime >= ${from}::timestamp` : empty;

    const countRows = await this.pgConnection.query<{ count: number }>(sql`
      SELECT count(*)::int as count
      FROM ${raw(this.incentivesTable)}
      WHERE
        status = ${IncentiveStatusEnum.Draft}::policy.incentive_status_enum
        ${fromFilter}
        AND datetime < ${to}::timestamp
    `);

    logger.debug(`FOUND ${countRows[0].count} incentives to process`);

    const query = sql`
      SELECT
        _id,
        policy_id,
        datetime,
        result as stateless_amount,
        amount as stateful_amount,
        status,
        state,
        operator_id,
        operator_journey_id,
        meta
      FROM ${raw(this.incentivesTable)}
      WHERE
        status = ${IncentiveStatusEnum.Draft}::policy.incentive_status_enum
        ${fromFilter}
        AND datetime <= ${to}::timestamp
      ORDER BY datetime ASC
    `;

    type DraftRow = {
      _id: number;
      policy_id: number;
      datetime: Date;
      stateless_amount: number;
      stateful_amount: number;
      status: IncentiveStatusEnum;
      state: IncentiveStateEnum;
      operator_id: number;
      operator_journey_id: string;
      meta: SerializedMetadataVariableDefinitionInterface[];
    };

    try {
      await using cursor = await this.pgConnection.cursor<DraftRow>(query);
      for await (const rows of cursor.read(batchSize)) {
        yield rows.map((r) => {
          const { stateful_amount, stateless_amount, ...other } = r;
          return {
            ...other,
            statefulAmount: stateful_amount,
            statelessAmount: stateless_amount,
          };
        });
        logger.log({ count: rows.length });
      }
    } catch (e) {
      if (e instanceof Error) {
        logger.error(`[IncentiveRepositoryProvider] ${e.message}`, { from, to });
      } else {
        logger.error(`[IncentiveRepositoryProvider] Unknown error`, { from, to });
      }
      throw e;
    }
  }

  async createOrUpdateMany(
    data: SerializedIncentiveInterface<undefined>[],
  ): Promise<void> {
    const idSet: Set<string> = new Set();
    const filteredData = data
      .reverse()
      .filter((d) => {
        const key = `${d.policy_id}/${d.operator_id}/${d.operator_journey_id}`;
        if (idSet.has(key)) {
          return false;
        }
        idSet.add(key);
        return true;
      })
      .map((i) => ({
        ...i,
        status: i.status || IncentiveStatusEnum.Draft,
        state: i.statefulAmount === 0 ? IncentiveStateEnum.Null : IncentiveStateEnum.Regular,
        meta: i.meta || {},
      }));

    const policyIds: number[] = [];
    const datetimes: Date[] = [];
    const statelessAmounts: number[] = [];
    const statefulAmounts: number[] = [];
    const statuses: IncentiveStatusEnum[] = [];
    const states: IncentiveStateEnum[] = [];
    const operatorIds: number[] = [];
    const operatorJourneyIds: string[] = [];
    const metas: string[] = [];

    for (const i of filteredData) {
      policyIds.push(i.policy_id);
      datetimes.push(i.datetime);
      statelessAmounts.push(i.statelessAmount);
      statefulAmounts.push(i.statefulAmount);
      statuses.push(i.status);
      states.push(i.state);
      operatorIds.push(i.operator_id);
      operatorJourneyIds.push(i.operator_journey_id);
      metas.push(JSON.stringify(i.meta));
    }

    await this.pgConnection.query(sql`
      WITH lowest_incentive AS (
        SELECT min(dtz) AS datetime
        FROM UNNEST(${datetimes}::timestamp with time zone[]) AS x(dtz)
      )
      --

      INSERT INTO ${raw(this.incentivesTable)} (
        policy_id,
        carpool_id,
        datetime,
        result,
        amount,
        status,
        state,
        operator_id,
        operator_journey_id,
        meta
      )
      SELECT
        unnest_data.policy_id,
        null as carpool_id,
        unnest_data.datetime,
        unnest_data.result,
        unnest_data.amount,
        unnest_data.status,
        unnest_data.state,
        unnest_data.operator_id,
        unnest_data.operator_journey_id,
        unnest_data.meta
      FROM UNNEST(
        ${policyIds}::int[],
        ${datetimes}::timestamp[],
        ${statelessAmounts}::int[],
        ${statefulAmounts}::int[],
        ${statuses}::policy.incentive_status_enum[],
        ${states}::policy.incentive_state_enum[],
        ${operatorIds}::int[],
        ${operatorJourneyIds}::varchar[],
        ${metas}::json[]
      ) AS unnest_data(policy_id, datetime, result, amount, status, state, operator_id, operator_journey_id, meta)
      ON CONFLICT (policy_id, operator_id, operator_journey_id)
      DO UPDATE SET
        result = excluded.result,
        amount = excluded.amount,
        status = excluded.status,
        state = excluded.state,
        meta = excluded.meta
    `);
  }

  async latestDraft(): Promise<Date> {
    const rows = await this.pgConnection.query<{ datetime: Date }>(sql`
      SELECT min(datetime) AS datetime
      FROM ${raw(this.incentivesTable)}
      WHERE status = ${IncentiveStatusEnum.Draft}::policy.incentive_status_enum
        AND datetime > current_timestamp - interval '1 year'
    `);
    return rows[0]?.datetime;
  }

  // TODO dedup from PolicyRepositoryProvider.syncIncentiveSum
  async updateIncentiveSum(): Promise<void> {
    await this.pgConnection.query(sql`
      UPDATE policy.policies p
      SET incentive_sum = policy_incentive_sum.amount
      FROM
        (SELECT policy_id, coalesce(sum(amount),0)::int AS amount
          FROM policy.incentives
          WHERE status = 'validated'
          GROUP BY policy_id) AS policy_incentive_sum
      WHERE p._id = policy_incentive_sum.policy_id
    `);
  }
}
