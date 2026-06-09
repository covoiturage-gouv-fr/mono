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

  start_datetime: string;
  start_date: string;
  start_time: string;
  end_datetime: string;
  end_date: string;
  end_time: string;
  duration: string;
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
  incentive_rpc_0_campaign_name: string;
  incentive_rpc_0_siret: string;
  incentive_rpc_0_name: string;
  incentive_rpc_0_amount: number;

  incentive_rpc_1_campaign_id: number;
  incentive_rpc_1_campaign_name: string;
  incentive_rpc_1_siret: string;
  incentive_rpc_1_name: string;
  incentive_rpc_1_amount: number;

  incentive_rpc_2_campaign_id: number;
  incentive_rpc_2_campaign_name: string;
  incentive_rpc_2_siret: string;
  incentive_rpc_2_name: string;
  incentive_rpc_2_amount: number;

  // Offer
  offer_public: boolean;
  offer_accepted_at: Date;
};

export function carpoolListQuery(params: ExportParams): Sql {
  const table = "refined_zone.export_carpool_list";
  const { start_at, end_at } = params.get();
  const geo_selectors = params.geoToSQL();
  const operator_id = params.operatorToSQL();

  return sql`
    SELECT *
    FROM ${raw(table)}
    WHERE true
      AND start_date_filter >= ${start_at}
      AND start_date_filter  < ${end_at}
      AND start_datetime_utc >= (${start_at}::date)::timestamp AT TIME ZONE 'Europe/Paris'
      AND start_datetime_utc  < (${end_at}::date)::timestamp   AT TIME ZONE 'Europe/Paris'
      ${raw(geo_selectors)}
      ${raw(operator_id)}
    ORDER BY start_date_filter ASC
  `;
}
