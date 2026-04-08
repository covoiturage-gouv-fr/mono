import { provider } from "@/ilos/common/index.ts";
import { TerritoryRepositoryInterfaceResolver } from "@/pdc/services/export/repositories/TerritoryRepository.ts";
import {
  TerritoryCodeEnum,
  TerritorySelectorsInterface,
} from "@/pdc/services/territory/contracts/common/interfaces/TerritoryCodeInterface.ts";
export type ResolveParams = Partial<{
  territory_id: number[];
  geo_selector: TerritorySelectorsInterface;
}>;
export type ResolveResults = TerritorySelectorsInterface;

export abstract class TerritoryServiceInterfaceResolver {
  public geoStringToObject(geo: string[]): TerritorySelectorsInterface {
    throw new Error("Not implemented");
  }
  public async resolve(params: ResolveParams): Promise<ResolveResults> {
    throw new Error("Not implemented");
  }
  public mergeSelectors(
    arr: TerritorySelectorsInterface[],
  ): TerritorySelectorsInterface {
    throw new Error("Not implemented");
  }
  public async getTerritoryNames(_geoSelector: TerritorySelectorsInterface | null): Promise<string[]> {
    throw new Error("Not implemented");
  }
}

@provider({
  identifier: TerritoryServiceInterfaceResolver,
})
export class TerritoryService {
  protected readonly defaultResolveResult = null;

  constructor(
    protected territoryRepository: TerritoryRepositoryInterfaceResolver,
  ) {}

  /**
   * Convert a geo_selector string to a geo_selector object
   *
   * @param geo
   * @returns
   */
  public geoStringToObject(geo: string[]): TerritorySelectorsInterface {
    const selectors = geo
      .reduce((p, c) => {
        const [type, code] = c
          .split(":")
          .map((s: string) => String(s).toLowerCase().trim()) as [
            keyof TerritorySelectorsInterface,
            string,
          ];

        if (type && code) {
          p[type] = p[type] || [];
          p[type] = [...p[type]!, code.toUpperCase()];
        }

        return p;
      }, {
        [TerritoryCodeEnum.City]: [],
        [TerritoryCodeEnum.Mobility]: [],
        [TerritoryCodeEnum.CityGroup]: [],
        [TerritoryCodeEnum.District]: [],
        [TerritoryCodeEnum.Region]: [],
        [TerritoryCodeEnum.Country]: [],
      } as TerritorySelectorsInterface);

    // clean up empty selectors
    Object
      .keys(selectors)
      .forEach((key: keyof TerritorySelectorsInterface) => {
        if (selectors[key]?.length === 0) {
          delete selectors[key];
        }
      });

    return Object.keys(selectors).length ? selectors : this.defaultResolveResult;
  }

  /**
   * Resolve to a geo_selector from a `territory_id` or a `geo_selector` string.
   *
   * `territory_id` might differ from an administrative geographical division.
   * When given both params, `geo_selector` takes precedence
   */
  public async resolve(params: ResolveParams): Promise<ResolveResults> {
    // select the whole country if all params are missing
    if (
      (!params.territory_id && !params.geo_selector) ||
      (Array.isArray(params.geo_selector) && params.geo_selector.length === 0)
    ) {
      return this.defaultResolveResult;
    }

    if (!params.geo_selector) {
      // get an array of selectors for each territory_id
      const territorySelectors = await Promise.all(
        (params.territory_id || []).map((id) => this.territoryRepository.getTerritorySelectors(id)),
      );

      // merge all selectors into one
      const merge = this.mergeSelectors(territorySelectors);
      if (Object.keys(merge).length > 0) {
        return merge;
      }

      // fallback to the default country
      return this.defaultResolveResult;
    }

    return params.geo_selector;
  }

  public mergeSelectors(
    arr: TerritorySelectorsInterface[],
  ): TerritorySelectorsInterface {
    return arr.reduce((acc, curr) => {
      Object.keys(curr).forEach((key) => {
        acc[key] = acc[key] || [];
        acc[key] = [...new Set([...acc[key]!, ...curr[key]!])];
      });
      return acc;
    }, {} as TerritorySelectorsInterface);
  }

  /**
   * Get all territory names from a geo_selector
   *
   * @param geoSelector
   * @returns Array of territory names
   */
  public async getTerritoryNames(geoSelector: TerritorySelectorsInterface | null): Promise<string[]> {
    if (!geoSelector) return [];

    const names: string[] = [];
    const types = Object.keys(geoSelector) as (keyof TerritorySelectorsInterface)[];

    for (const type of types) {
      const codes = geoSelector[type];
      if (!codes || !codes.length) continue;

      const batchNames = await this.territoryRepository.getTerritoryNamesBatch(type as string, codes);
      names.push(...batchNames);
    }

    return names;
  }
}
