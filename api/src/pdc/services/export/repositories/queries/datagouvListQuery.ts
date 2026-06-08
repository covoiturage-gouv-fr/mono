import sql, { raw, Sql } from "@/lib/pg/sql.ts";
import { ExportParams } from "@/pdc/services/export/models/ExportParams.ts";
import { DataGouvQueryConfig } from "@/pdc/services/export/repositories/CarpoolRepository.ts";

export type DataGouvListType = {
  journey_id: number;
  trip_id: string;

  journey_start_datetime: string;
  journey_start_date: string;
  journey_start_time: string;
  journey_start_lon: number;
  journey_start_lat: number;
  journey_start_insee: string;
  journey_start_department: string;
  journey_start_town: string;
  journey_start_towngroup: string;
  journey_start_country: string;

  journey_end_datetime: string;
  journey_end_date: string;
  journey_end_time: string;
  journey_end_lon: number;
  journey_end_lat: number;
  journey_end_insee: string;
  journey_end_department: string;
  journey_end_town: string;
  journey_end_towngroup: string;
  journey_end_country: string;

  passenger_seats: number;
  operator_class: string;
  journey_distance: number;
  journey_duration: number;
  has_incentive: boolean;
};

export function datagouvListQuery(params: ExportParams, config: DataGouvQueryConfig): Sql {
  const table = "refined_zone.export_opendata_list";
  const { start_at, end_at } = params.get();
  const { min_occurrences } = config;

  // The start_datetime bounds below are the sargable equivalent of the
  // start_date_filter range: paris_date(t) in [start, end) iff
  // t in [paris_midnight(start), paris_midnight(end)). They let the planner
  // use the journeys (start_datetime) index instead of a full seq scan; the
  // start_date_filter predicates stay as the authoritative (exact) filter.
  return sql`
    SELECT *
    FROM ${raw(table)}
    WHERE start_date_filter >= ${start_at}
      AND start_date_filter  < ${end_at}
      AND start_datetime >= (${start_at}::date)::timestamp AT TIME ZONE 'Europe/Paris'
      AND start_datetime  < (${end_at}::date)::timestamp   AT TIME ZONE 'Europe/Paris'
      AND start_insee_count >= ${min_occurrences}
      AND end_insee_count   >= ${min_occurrences}
    ORDER BY start_date_filter ASC
  `;
}
