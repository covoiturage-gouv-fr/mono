import sql, { raw, Sql } from "@/lib/pg/sql.ts";
import { ExportParams } from "@/pdc/services/export/models/ExportParams.ts";

export function carpoolCountQuery(params: ExportParams): Sql {
  const table = "refined_zone.export_carpool_list";
  const { start_at, end_at } = params.get();
  const geo_selectors = params.geoToSQL();
  const operator_id = params.operatorToSQL();

  return sql`
    SELECT count(cc.*) as count
    FROM ${raw(table)} cc
    WHERE true
      AND cc.start_date_filter >= ${start_at}
      AND cc.start_date_filter  < ${end_at}
        ${raw(geo_selectors)}
        ${raw(operator_id)}
    `;
}
