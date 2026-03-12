import sql, { raw, Sql } from "@/lib/pg/sql.ts";
import {
  CarpoolAcquisitionStatusEnum,
  CarpoolAnomalyStatusEnum,
  CarpoolFraudStatusEnum,
} from "@/pdc/providers/carpool/interfaces/common.ts";
import { ExportParams } from "@/pdc/services/export/models/ExportParams.ts";

export type TemplateKeys = "geo_selectors" | "operator_id";

export type CarpoolListType = {
  journey_id: number;
  operator_trip_id: string;
  operator_journey_id: string;
  operator_class: string;
  acquisition_status: CarpoolAcquisitionStatusEnum;
  fraud_status: CarpoolFraudStatusEnum;
  anomaly_status: CarpoolAnomalyStatusEnum;

  start_datetime: Date;
  start_date: string;
  start_time: string;
  end_datetime: Date;
  end_date: string;
  end_time: string;
  duration: number;
  distance: number;

  start_lat: number;
  start_lon: number;
  end_lat: number;
  end_lon: number;

  start_insee: string;
  start_commune: string;
  start_departement: string;
  start_epci: string;
  start_aom: string;
  start_region: string;
  start_pays: string;

  end_insee: string;
  end_commune: string;
  end_departement: string;
  end_epci: string;
  end_aom: string;
  end_region: string;
  end_pays: string;

  operator: string;
  operator_passenger_id: string;
  passenger_identity_key: string;
  operator_driver_id: string;
  driver_identity_key: string;

  driver_revenue: number;
  passenger_contribution: number;
  passenger_seats: number;

  cee_application: boolean;

  // incentives
  incentive_0_siret: string;
  incentive_0_name: string;
  incentive_0_amount: number;

  incentive_1_siret: string;
  incentive_1_name: string;
  incentive_1_amount: number;

  incentive_2_siret: string;
  incentive_2_name: string;
  incentive_2_amount: number;

  // RPC incentives
  incentive_rpc_0_campaign_id: number;
  incentive_rpc_0_campaign_name: number;
  incentive_rpc_0_siret: string;
  incentive_rpc_0_name: string;
  incentive_rpc_0_amount: number;

  incentive_rpc_1_campaign_id: number;
  incentive_rpc_1_campaign_name: number;
  incentive_rpc_1_siret: string;
  incentive_rpc_1_name: string;
  incentive_rpc_1_amount: number;

  incentive_rpc_2_campaign_id: number;
  incentive_rpc_2_campaign_name: number;
  incentive_rpc_2_siret: string;
  incentive_rpc_2_name: string;
  incentive_rpc_2_amount: number;

  // Offer
  offer_public: boolean;
  offer_accepted_at: Date;
};

export function carpoolListQuery(params: ExportParams): Sql {
  const table = "refined_zone.export_carpool_list";
  const { start_at, end_at, tz } = params.get();
  const geo_selectors = params.geoToSQL();
  const operator_id = params.operatorToSQL();

  return sql`
    SELECT
      -- general trip identifiers
      journey_id,
      operator_trip_id,
      operator_journey_id,
      operator_class,
      acquisition_status,
      fraud_status,
      anomaly_status,

      -- dates and times are in UTC
      -- ceil times to 10 minutes and format for user's convenience
      to_char(ts_ceil(start_datetime at time zone ${tz}, 600), 'YYYY-MM-DD HH24:MI:SS') as start_datetime,
      to_char(ts_ceil(start_datetime at time zone ${tz}, 600), 'YYYY-MM-DD') as start_date,
      to_char(ts_ceil(start_datetime at time zone ${tz}, 600), 'HH24:MI:SS') as start_time,
      to_char(ts_ceil(end_datetime at time zone ${tz}, 600), 'YYYY-MM-DD HH24:MI:SS') as end_datetime,
      to_char(ts_ceil(end_datetime at time zone ${tz}, 600), 'YYYY-MM-DD') as end_date,
      to_char(ts_ceil(end_datetime at time zone ${tz}, 600), 'HH24:MI:SS') as end_time,
      to_char((duration || ' seconds')::interval, 'HH24:MI:SS') as duration,

      distance,

      -- truncate position depending on population density
      start_lat,
      start_lon,
      end_lat,
      end_lon,

      -- administrative data
      start_insee,
      start_commune,
      start_departement,
      start_epci,
      start_aom,
      start_region,
      start_pays,

      end_insee,
      end_commune,
      end_departement,
      end_epci,
      end_aom,
      end_region,
      end_pays,

      -- operator data
      operator,
      operator_passenger_id,
      passenger_identity_key,
      operator_driver_id,
      driver_identity_key,

      -- financial data
      driver_revenue,
      passenger_contribution,
      passenger_seats,

      -- CEE application data
      cee_application,

      -- incentives
      incentive[0]->>'siret' as incentive_0_siret,
      incentive[0]->>'name' as incentive_0_name,
      incentive[0]->>'amount' as incentive_0_amount,

      incentive[1]->>'siret' as incentive_1_siret,
      incentive[1]->>'name' as incentive_1_name,
      incentive[1]->>'amount' as incentive_1_amount,

      incentive[2]->>'siret' as incentive_2_siret,
      incentive[2]->>'name' as incentive_2_name,
      incentive[2]->>'amount' as incentive_2_amount,

      -- RPC incentives
      incentive_rpc[0]->>'campaign_id' as incentive_rpc_0_campaign_id,
      incentive_rpc[0]->>'campaign_name' as incentive_rpc_0_campaign_name,
      incentive_rpc[0]->>'siret' as incentive_rpc_0_siret,
      incentive_rpc[0]->>'name' as incentive_rpc_0_name,
      incentive_rpc[0]->>'amount' as incentive_rpc_0_amount,

      incentive_rpc[1]->>'campaign_id' as incentive_rpc_1_campaign_id,
      incentive_rpc[1]->>'campaign_name' as incentive_rpc_1_campaign_name,
      incentive_rpc[1]->>'siret' as incentive_rpc_1_siret,
      incentive_rpc[1]->>'name' as incentive_rpc_1_name,
      incentive_rpc[1]->>'amount' as incentive_rpc_1_amount,

      incentive_rpc[2]->>'campaign_id' as incentive_rpc_2_campaign_id,
      incentive_rpc[2]->>'campaign_name' as incentive_rpc_2_campaign_name,
      incentive_rpc[2]->>'siret' as incentive_rpc_2_siret,
      incentive_rpc[2]->>'name' as incentive_rpc_2_name,
      incentive_rpc[2]->>'amount' as incentive_rpc_2_amount,

      -- offer data (to be completed)
      true as offer_public, -- TODO
      start_datetime as offer_accepted_at -- TODO

    FROM ${raw(table)}
    WHERE true
      AND start_datetime >= ${start_at}
      AND start_datetime <  ${end_at}
        ${raw(geo_selectors)}
        ${raw(operator_id)}
    ORDER BY start_datetime ASC
  `;
}
