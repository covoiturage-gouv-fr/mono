import { provider } from "@/ilos/common/index.ts";
import { Cursor, DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import { set } from "@/lib/object/index.ts";
import sql, { empty, raw } from "@/lib/pg/sql.ts";
import { UnboundedSlices } from "../../policy/contracts/common/interfaces/Slices.ts";
import { PolicyStatsInterface } from "../contracts/interfaces/PolicySliceStatInterface.ts";
import {
  CampaignSearchParamsInterface,
  DataRepositoryInterface,
  DataRepositoryProviderInterfaceResolver,
} from "../interfaces/APDFRepositoryProviderInterface.ts";
import { APDFTripInterface } from "../interfaces/APDFTripInterface.ts";

type PolicyStatsRow = {
  total_count: number;
  total_sum: number;
  subsidized_count: number;
  [slice_key: string]: number | string;
};

@provider({ identifier: DataRepositoryProviderInterfaceResolver })
export class DataRepositoryProvider implements DataRepositoryInterface {
  protected readonly carpoolV2Table = "carpool_v2.carpools";
  protected readonly carpoolV2StatusTable = "carpool_v2.status";
  protected readonly carpoolV2GeoTable = "carpool_v2.geo";
  protected readonly policyIncentivesTable = "policy.incentives";
  protected readonly geoPerimetersTable = "geo.perimeters";
  protected readonly operatorsTable = "operator.operators";

  constructor(public pgConnection: DenoPostgresConnection) {
  }

  /**
   * List active operators having trips and incentives > 0
   * in the campaign for the given date range
   */
  public async getPolicyActiveOperators(
    campaign_id: number,
    start_date: Date,
    end_date: Date,
  ): Promise<number[]> {
    const rows = await this.pgConnection.query<{ operator_id: number }>(sql`
      SELECT cc.operator_id
      FROM ${raw(this.policyIncentivesTable)} pi
      JOIN ${raw(this.carpoolV2Table)} cc
        ON cc.operator_id = pi.operator_id
        AND cc.operator_journey_id = pi.operator_journey_id
      JOIN ${raw(this.carpoolV2StatusTable)} cs
        ON cc._id = cs.carpool_id
      WHERE pi.policy_id = ${campaign_id}
        AND pi.amount    > 0
        AND cc.start_datetime >= ${start_date}
        AND cc.start_datetime  < ${end_date}
        AND cs.acquisition_status = 'processed'
        AND cs.fraud_status = 'passed'
        AND cs.anomaly_status = 'passed'
      GROUP BY cc.operator_id
      ORDER BY cc.operator_id
    `);

    return rows.map((r) => r.operator_id);
  }

  /**
   * Compile stats on carpools applying slices
   */
  public async getPolicyStats(
    params: CampaignSearchParamsInterface,
    slices: UnboundedSlices | [],
  ): Promise<PolicyStatsInterface> {
    const { start_date, end_date, operator_id, campaign_id } = params;

    // Build slice filter expressions. start/end come from the campaign
    // configuration and are validated as numbers before reaching here, so
    // inlining them is safe.
    const sliceFilters: string = slices
      .map(({ start, end }, i: number) => {
        const f = `filter (where distance >= ${start}${end ? ` and distance < ${end}` : ""})`;
        return `
          (count(uuid) ${f})::int as slice_${i}_count,
          (count(uuid) ${f.replace("where", "where amount > 0 and")})::int as slice_${i}_subsidized,
          (sum(amount) ${f})::int as slice_${i}_sum,
          ${start} as slice_${i}_start,
          ${end ? end : "'Infinity'"} as slice_${i}_end
        `;
      })
      .join(",");

    // select all trips with a positive incentive calculated by us for a given campaign
    // calculate a global count and incentive sum as well as details for each slice
    const rows = await this.pgConnection.query<PolicyStatsRow>(sql`
      with trips as (
        select
            cc.uuid,
            cc.distance,
            coalesce(pi.amount, 0) as amount
        from ${raw(this.policyIncentivesTable)} pi
        join ${raw(this.carpoolV2Table)} cc
          on  cc.operator_id = pi.operator_id
          and cc.operator_journey_id = pi.operator_journey_id
        join ${raw(this.carpoolV2StatusTable)} cs on cs.carpool_id = cc._id
      where
              cc.start_datetime >= ${start_date}
          and cc.start_datetime  < ${end_date}
          and cs.acquisition_status in ('processed', 'canceled', 'updated')
          and cs.fraud_status       in ('passed', 'failed')
          and cc.operator_id = ${operator_id}
          and pi.policy_id = ${campaign_id}
          and pi.status = 'validated'
          and pi.amount >= 0
        )
      select
        count(uuid)::int as total_count,
        sum(amount)::int as total_sum,
        (count(uuid) filter (where amount > 0))::int as subsidized_count
        ${raw(sliceFilters.length ? `, ${sliceFilters}` : "")}
      from trips
    `);

    // return null results on missing data
    if (!rows.length) {
      return {
        total_count: 0,
        total_sum: 0,
        subsidized_count: 0,
        slices: [],
      };
    }

    // rearrange the return object
    const row = rows[0];
    return Object.keys(row).reduce(
      (p, k) => {
        if (!k.includes("slice_")) return p;
        const [, i, prop] = k.split("_");
        if (prop === "start") {
          set(p, `slices.${i}.slice.start`, row[k]);
        } else if (prop === "end") {
          // Highest slice can return Infinity as boundary
          set(
            p,
            `slices.${i}.slice.end`,
            row[k] === "Infinity" ? undefined : row[k],
          );
        } else {
          set(p, `slices.${i}.${prop}`, row[k]);
        }
        return p;
      },
      {
        total_count: row.total_count,
        total_sum: row.total_sum,
        subsidized_count: row.subsidized_count,
        slices: [],
      },
    );
  }

  /**
   * List all carpools for CSV APDF export using a cursor
   */
  public async getPolicyCursor(params: CampaignSearchParamsInterface): Promise<Cursor<APDFTripInterface>> {
    const { start_date, end_date, operator_id, campaign_id, declared_siren } = params;

    // Incitation déclarée par l'opérateur, rattachée au territoire financeur par le
    // SIREN (9 chiffres) — cf. GEN-643. Uniquement quand la campagne l'expose.
    const declaredSelect = declared_siren
      ? sql`decl.amount as operator_declared_incentive,`
      : sql`null::int as operator_declared_incentive,`;
    const declaredJoin = declared_siren
      ? sql`
        left join lateral (
          select sum(oi.amount) as amount
          from carpool_v2.operator_incentives oi
          where oi.carpool_id = cc._id
            and left(oi.siret, 9) = ${declared_siren}
        ) decl on true`
      : empty;

    return this.pgConnection.cursor<APDFTripInterface>(sql`
      -- list in api/services/trip/src/providers/excel/TripsWorksheetWriter.ts
      select
        cc.uuid as rpc_journey_id,
        cc.operator_journey_id,
        cc.operator_trip_id,

        cc.driver_operator_user_id,
        cc.passenger_operator_user_id,
        cc.passenger_contribution,

        pi.amount as rpc_incentive,
        ${declaredSelect}

        cc.start_datetime,
        cc.end_datetime,

        gps.l_arr as start_location,
        gps.arr as start_insee,
        gps.l_epci as start_epci,
        gpe.l_arr as end_location,
        gpe.arr as end_insee,
        gpe.l_epci as end_epci,

        to_char(cc.end_datetime - cc.start_datetime, 'HH24:MI:SS') as duration,
        cc.distance,

        oo.name as operator,
        cc.operator_class

      from ${raw(this.policyIncentivesTable)} pi
      join ${raw(this.carpoolV2Table)} cc on cc.operator_id = pi.operator_id and cc.operator_journey_id = pi.operator_journey_id
      join ${raw(this.carpoolV2StatusTable)} cs on cc._id = cs.carpool_id
      join ${raw(this.carpoolV2GeoTable)} cg on cc._id = cg.carpool_id
      left join ${raw(this.geoPerimetersTable)} gps on cg.start_geo_code = gps.arr and gps.year = geo.get_latest_millesime_or(extract(year from cc.start_datetime)::smallint)
      left join ${raw(this.geoPerimetersTable)} gpe on cg.end_geo_code = gpe.arr and gpe.year = geo.get_latest_millesime_or(extract(year from cc.end_datetime)::smallint)
      left join ${raw(this.operatorsTable)} oo on oo._id = cc.operator_id
      ${declaredJoin}

      where
            cc.start_datetime >= ${start_date}
        and cc.start_datetime  < ${end_date}
        and cs.acquisition_status in ('processed', 'canceled', 'updated')
        and cs.fraud_status in ('passed', 'failed')
        and cc.operator_id = ${operator_id}
        and pi.policy_id = ${campaign_id}
        and pi.status = 'validated'
        and pi.amount >= 0

      order by cc.start_datetime
    `);
  }
}
