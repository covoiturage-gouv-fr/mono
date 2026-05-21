import { provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { empty, raw } from "@/lib/pg/sql.ts";
import { CarpoolInterface, PolicyInterface, TripRepositoryProviderInterfaceResolver } from "../interfaces/index.ts";

@provider({
  identifier: TripRepositoryProviderInterfaceResolver,
})
export class TripRepositoryProvider implements TripRepositoryProviderInterfaceResolver {
  public readonly table = "carpool_v2.carpools";
  public readonly geoTable = "carpool_v2.geo";
  public readonly statusTable = "carpool_v2.status";
  public readonly operatorTable = "operator.operators";
  public readonly incentiveTable = "policy.incentives";
  public readonly getComFunction = "territory.get_com_by_territory_id";
  public readonly getMillesimeFunction = "geo.get_latest_millesime";

  constructor(protected pgConnection: DenoPostgresConnection) {}

  async *findTripByGeo(
    coms: string[],
    from: Date,
    to: Date,
    batchSize = 300,
    override = true,
    policy_id?: number,
  ): AsyncGenerator<CarpoolInterface[], void, void> {
    const overrideJoin = override ? empty : sql`
      LEFT JOIN ${raw(this.incentiveTable)} pi
        ON
          cc.operator_journey_id = pi.operator_journey_id
          AND cc.operator_id = pi.operator_id
          AND pi.policy_id = ${policy_id}::int
    `;
    const overrideFilter = override ? empty : sql`AND pi._id IS NULL`;

    const query = sql`
      SELECT
        oo.uuid as operator_uuid,
        cc.operator_trip_id,
        cc.operator_id,
        cc.operator_journey_id,
        cc.operator_class,
        cc.passenger_contribution,
        cc.passenger_identity_key,
        cc.passenger_travelpass_user_id IS NOT NULL as passenger_has_travel_pass,
        cc.passenger_over_18 as passenger_is_over_18,
        cc.passenger_seats as seats,
        cc.driver_revenue,
        cc.driver_identity_key,
        cc.driver_travelpass_user_id IS NOT NULL as driver_has_travel_pass,
        cc.start_datetime as datetime,
        cc.distance,
        row_to_json(
          geo.get_by_code(
            co.start_geo_code::varchar,
            geo.get_latest_millesime_or(EXTRACT(year FROM cc.start_datetime)::smallint)
          )
        ) as start,
        row_to_json(
          geo.get_by_code(
            co.end_geo_code::varchar,
            geo.get_latest_millesime_or(EXTRACT(year FROM cc.start_datetime)::smallint)
          )
        ) as end,
        ST_X(cc.start_position::geometry)::numeric as start_lon,
        ST_Y(cc.start_position::geometry)::numeric as start_lat,
        ST_X(cc.end_position::geometry)::numeric as end_lon,
        ST_Y(cc.end_position::geometry)::numeric as end_lat
      FROM ${raw(this.table)} cc
      JOIN ${raw(this.geoTable)} co
        ON co.carpool_id = cc._id
      JOIN ${raw(this.operatorTable)} oo
        ON oo._id = cc.operator_id
      JOIN ${raw(this.statusTable)} cs
        ON cs.carpool_id = cc._id
      ${overrideJoin}
      WHERE
        cc.start_datetime >= ${from}::timestamp
        AND cc.start_datetime < ${to}::timestamp
        AND (
          co.start_geo_code = ANY(${coms}::varchar[])
          OR co.end_geo_code = ANY(${coms}::varchar[])
        )
        ${overrideFilter}
      ORDER BY cc.start_datetime ASC
    `;
    // TODO status

    await using cursor = await this.pgConnection.cursor<CarpoolInterface>(query);
    for await (const rows of cursor.read(batchSize)) {
      yield rows;
    }
  }

  async *findTripByPolicy(
    policy: PolicyInterface,
    from: Date,
    to: Date,
    batchSize = 100,
    override = false,
  ): AsyncGenerator<CarpoolInterface[], void, void> {
    const yearRows = await this.pgConnection.query<{ year: number }>(sql`
      SELECT * from ${raw(this.getMillesimeFunction)}() as year
    `);
    const year = yearRows[0]?.year;

    const comRows = await this.pgConnection.query<{ com: string }>(sql`
      SELECT * FROM ${raw(this.getComFunction)}(${policy.territory_id}::int, ${year}::smallint)
    `);

    const com: string[] = comRows.map((r) => r.com);

    yield* this.findTripByGeo(com, from, to, batchSize, override, policy._id);
  }
}
