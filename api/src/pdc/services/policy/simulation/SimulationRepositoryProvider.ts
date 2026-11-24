import { provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql from "@/lib/pg/sql.ts";
import { IncentiveStatusEnum } from "../interfaces/index.ts";

export interface CurrentIncentive {
  operator_id: number;
  operator_journey_id: string;
  amount: number;
  status: IncentiveStatusEnum;
}

/** Read-only queries of the campaign:apply dry-run. */
@provider()
export class SimulationRepositoryProvider {
  constructor(protected pgConnection: DenoPostgresConnection) {}

  async findCurrentIncentives(policy_id: number, from: Date, to: Date): Promise<CurrentIncentive[]> {
    return await this.pgConnection.query<CurrentIncentive>(sql`
      SELECT operator_id, operator_journey_id, amount, status
      FROM policy.incentives
      WHERE policy_id = ${policy_id}
        AND datetime >= ${from}::timestamp
        AND datetime < ${to}::timestamp
    `);
  }

  /** SELECT twin of IncentiveRepositoryProvider.disableOnExcludedCarpool */
  async findExcludedCarpools(
    from: Date,
    to: Date,
  ): Promise<Array<{ operator_id: number; operator_journey_id: string }>> {
    return await this.pgConnection.query(sql`
      SELECT cc.operator_id, cc.operator_journey_id
      FROM carpool_v2.carpools AS cc
      JOIN carpool_v2.status AS cs
        ON cs.carpool_id = cc._id
      WHERE cc.start_datetime >= ${from}::timestamp
        AND cc.start_datetime <  ${to}::timestamp
        AND (
          cs.acquisition_status = 'canceled'
          OR cs.anomaly_status = 'failed'
          OR cs.fraud_status = 'failed'
        )
    `);
  }
}
