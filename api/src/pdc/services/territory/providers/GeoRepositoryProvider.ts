import { KernelInterfaceResolver, provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { empty, raw } from "@/lib/pg/sql.ts";
import {
  ResultInterface as AllGeoResultInterface,
  SingleResultInterface as GeoResultInterface,
} from "@/pdc/services/territory/contracts/allGeo.contract.ts";
import { TerritoryCodeEnum } from "@/pdc/services/territory/contracts/common/interfaces/TerritoryCodeInterface.ts";
import {
  ParamsInterface as FindBySirenParamsInterface,
  ResultInterface as FindBySirenResultInterface,
} from "@/pdc/services/territory/contracts/findGeoBySiren.contract.ts";
import {
  ParamsInterface as ListGeoParamsInterface,
  ResultInterface as ListGeoResultInterface,
  SingleResultInterface as ListGeoSingleResultInterface,
} from "@/pdc/services/territory/contracts/listGeo.contract.ts";
import { FindBySiretRawResultInterface } from "../interfaces/FindBySiretRawResultInterface.ts";
import {
  GeoRepositoryProviderInterface,
  GeoRepositoryProviderInterfaceResolver,
} from "../interfaces/GeoRepositoryProviderInterface.ts";

@provider({
  identifier: GeoRepositoryProviderInterfaceResolver,
})
export class GeoRepositoryProvider implements GeoRepositoryProviderInterface {
  public readonly table = "geo.perimeters";
  public readonly tableCentroid = "geo.perimeters_centroid";
  public readonly relationTable = "territory.territory_group_selector";
  public readonly getMillesimeFunction = "geo.get_latest_millesime";

  constructor(
    protected pgConnection: DenoPostgresConnection,
    protected kernel: KernelInterfaceResolver,
  ) {}

  async getAllGeo(): Promise<AllGeoResultInterface> {
    return await this.pgConnection.query<GeoResultInterface>(sql`
      SELECT
        concat(territory, '_', type, '_', year) as id,
        territory,
        l_territory,
        type,
        year,
        year = geo.get_latest_millesime() as is_latest
      FROM ${raw(this.tableCentroid)}
      ORDER BY type, territory, year
    `);
  }

  async list(params: ListGeoParamsInterface): Promise<ListGeoResultInterface> {
    const { search, where: whereParams, exclude_coms, limit, offset } = {
      limit: 100,
      offset: 0,
      ...params,
    };
    const pattern = `%${search.toLowerCase().trim()}%`;

    const yearRows = await this.pgConnection.query<{ year: number }>(sql`
      SELECT * from ${raw(this.getMillesimeFunction)}() as year
    `);
    const year = yearRows[0]?.year;

    const inseeFilter = whereParams && whereParams.insee && whereParams.insee.length
      ? sql`AND arr = ANY(${whereParams.insee})`
      : empty;

    const comLikeBranch = exclude_coms ? empty : sql`OR lower(l_com) LIKE ${pattern}`;
    const comProjection = exclude_coms ? empty : sql`l_com, com,`;
    const comUnion = exclude_coms
      ? empty
      : sql`UNION SELECT DISTINCT l_com AS name, com AS insee, ${TerritoryCodeEnum.City} AS type FROM search WHERE lower(l_com) LIKE ${pattern}`;

    const searchCte = sql`
      SELECT aom, l_aom, epci, l_epci, ${comProjection} l_reg, reg, l_dep, dep FROM ${raw(this.table)}
      WHERE
        (
          l_aom LIKE ${pattern}
          ${comLikeBranch}
          OR lower(l_epci) LIKE ${pattern}
          OR lower(l_reg) LIKE ${pattern}
          OR lower(l_dep) LIKE ${pattern}
        )
        AND year = ${year}
        ${inseeFilter}
      ORDER BY YEAR DESC
    `;

    const totalRows = await this.pgConnection.query<{ count: number }>(sql`
      WITH search AS (${searchCte})
      SELECT count(*)::int AS count FROM (
        SELECT DISTINCT l_aom AS name, aom AS insee, ${TerritoryCodeEnum.Mobility} AS type FROM search WHERE lower(l_aom) LIKE ${pattern}
        UNION
        SELECT DISTINCT l_epci AS name, epci AS insee, ${TerritoryCodeEnum.CityGroup} AS type FROM search WHERE lower(l_epci) LIKE ${pattern}
        UNION
        SELECT DISTINCT l_reg AS name, reg AS insee, ${TerritoryCodeEnum.Region} AS type FROM search WHERE lower(l_reg) LIKE ${pattern}
        ${comUnion}
        UNION
        SELECT DISTINCT l_dep AS name, dep AS insee, ${TerritoryCodeEnum.District} AS type FROM search WHERE lower(l_dep) LIKE ${pattern}
      ) AS x
    `);

    const total = totalRows[0]?.count ?? 0;

    const results = await this.pgConnection.query<ListGeoSingleResultInterface>(sql`
      WITH search AS (${searchCte})
      SELECT DISTINCT l_aom AS name, aom AS insee, ${TerritoryCodeEnum.Mobility} AS type from search WHERE lower(l_aom) LIKE ${pattern}
      UNION
      SELECT DISTINCT l_epci AS name, epci AS insee, ${TerritoryCodeEnum.CityGroup} AS type from search WHERE lower(l_epci) LIKE ${pattern}
      UNION
      SELECT DISTINCT l_reg AS name, reg AS insee, ${TerritoryCodeEnum.Region} AS type from search WHERE lower(l_reg) LIKE ${pattern}
      ${comUnion}
      UNION
      SELECT DISTINCT l_dep AS name, dep AS insee, ${TerritoryCodeEnum.District} AS type from search WHERE lower(l_dep) LIKE ${pattern}
      ORDER BY name ASC
      LIMIT ${limit}
      OFFSET ${offset}
    `);

    // ResultWithPagination<T>.data is T[], so the declared
    // ResultInterface = ResultWithPagination<SingleResultInterface[]>
    // claims data is SingleResultInterface[][]. Pre-existing contract
    // bug; runtime has always returned a flat SingleResultInterface[].
    return {
      data: results as unknown as ListGeoSingleResultInterface[][],
      meta: {
        pagination: {
          offset,
          limit,
          total,
        },
      },
    };
  }

  async findBySiren(
    params: FindBySirenParamsInterface,
  ): Promise<FindBySirenResultInterface> {
    const yearRows = await this.pgConnection.query<{ year: number }>(sql`
      SELECT * from ${raw(this.getMillesimeFunction)}() as year
    `);
    const year = yearRows[0]?.year;

    const sirenFilter = (params.siren.length === 2 || params.siren.length === 3)
      ? sql`(reg = ${params.siren}::varchar OR dep = ${params.siren}::varchar)`
      : sql`(aom = ${params.siren}::varchar OR epci = ${params.siren}::varchar)`;

    const rows = await this.pgConnection.query<FindBySiretRawResultInterface>(sql`
      SELECT l_aom, aom, epci, l_epci, com, l_com, l_reg, reg, l_dep, dep
      FROM geo.perimeters
      WHERE ${sirenFilter}
        AND year = ${year}::int
    `);

    if (!rows[0]) {
      return {
        reg_name: null,
        reg_siren: null,
        aom_name: null,
        dep_name: null,
        dep_siren: null,
        aom_siren: null,
        epci_name: null,
        epci_siren: null,
        coms: [],
      };
    }

    return {
      reg_name: rows[0].l_reg,
      reg_siren: rows[0].reg,
      aom_name: rows[0].l_aom,
      aom_siren: rows[0].aom,
      dep_name: rows[0].l_dep,
      dep_siren: rows[0].dep,
      epci_name: rows[0].l_epci,
      epci_siren: rows[0].epci,
      coms: rows.map<Pick<ListGeoSingleResultInterface, "insee" | "name">>((g) => ({
        insee: g.com,
        name: g.l_com,
      })),
    };
  }
}
