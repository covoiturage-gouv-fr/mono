import sql, { raw, Sql } from "@/lib/pg/sql.ts";
import { ExportParams } from "@/pdc/services/export/models/ExportParams.ts";
import { DataGouvQueryConfig } from "@/pdc/services/export/repositories/CarpoolRepository.ts";

/**
 * Nombre trajets collectés et validés par le registre de preuve de covoiturage 1025610
 * Nombre de trajets exposés dans le jeu de données : 998086
 * Nombre de trajets supprimés du jeu de données : 27524 = 14958 + 15373 - 2807
 *     Nombre de trajets dont l’occurrence du code INSEE de départ est < 6 : 14958
 *     Nombre de trajets dont l’occurrence du code INSEE d’arrivée est < 6 : 15373
 *     Nombre de trajets dont l’occurrence du code INSEE de départ ET d’arrivée est < 6 : 2807
 */
export type DataGouvStatsType = {
  start_at: Date;
  end_at: Date;
  count_total: number;
  count_exposed: number;
  count_removed: number;
  count_removed_start: number;
  count_removed_end: number;
  count_removed_both: number;
};

export function datagouvStatsQuery(params: ExportParams, config: DataGouvQueryConfig): Sql {
  const table = "trusted_zone.insee_counters";
  const { start_at, end_at } = params.get();
  const { min_occurrences } = config;

  return sql`
    SELECT
      ${start_at}::timestamp as start_at,
      ${end_at}::timestamp as end_at,
      count(*) as count_total,
      count(*) FILTER (WHERE start_insee_count >= ${min_occurrences} AND end_insee_count >= ${min_occurrences}) as count_exposed,
      count(*) FILTER (WHERE start_insee_count < ${min_occurrences} OR end_insee_count < ${min_occurrences}) as count_removed,
      count(*) FILTER (WHERE start_insee_count < ${min_occurrences}) as count_removed_start,
      count(*) FILTER (WHERE end_insee_count < ${min_occurrences}) as count_removed_end,
      count(*) FILTER (WHERE start_insee_count < ${min_occurrences} AND end_insee_count < ${min_occurrences}) as count_removed_both
    FROM ${raw(table)}
    WHERE start_datetime >= ${start_at}
      AND start_datetime < ${end_at}
  `;
}
