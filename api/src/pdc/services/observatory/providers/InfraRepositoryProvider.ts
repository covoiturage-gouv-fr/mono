import { provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { join, raw } from "@/lib/pg/sql.ts";
import { checkTerritoryParam } from "../helpers/checkParams.ts";
import {
  AiresCovoiturageParamsInterface,
  AiresCovoiturageResultInterface,
  InfraRepositoryInterface,
  InfraRepositoryInterfaceResolver,
} from "../interfaces/InfraRepositoryProviderInterface.ts";

@provider({
  identifier: InfraRepositoryInterfaceResolver,
})
export class InfraRepositoryProvider implements InfraRepositoryInterface {
  private readonly table = "observatory.aires_covoiturage";
  private readonly perim_table = "geo.perimeters";
  constructor(private pgConnection: DenoPostgresConnection) {}

  async getAiresCovoiturage(
    params: AiresCovoiturageParamsInterface,
  ): Promise<AiresCovoiturageResultInterface> {
    const typeParam = checkTerritoryParam(params.type);
    const filters = [
      sql`ouvert = true`,
    ];
    if (params.code) {
      filters.push(sql`insee IN (
        SELECT arr 
        FROM ${raw(this.perim_table)}
        WHERE year = geo.get_latest_millesime() 
        AND ${raw(typeParam)} = ${params.code})`);
    }

    const query = sql`SELECT 
      id_lieu, 
      nom_lieu, 
      com_lieu, 
      type, 
      date_maj, 
      nbre_pl, 
      nbre_pmr, 
      duree, 
      horaires, 
      proprio, 
      lumiere, 
      ST_AsGeoJSON(geom,6)::json as geom 
      FROM ${raw(this.table)}
      WHERE ${join(filters, " AND ")}
    `;
    return await this.pgConnection.query<AiresCovoiturageResultInterface[number]>(query);
  }
}
