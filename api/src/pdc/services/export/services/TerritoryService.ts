import { NotFoundException, provider } from "@/ilos/common/index.ts";
import { ExportGeoSelectorInterface } from "@/pdc/services/export/contracts/create.contract.ts";
import { TerritoryRepositoryInterfaceResolver } from "@/pdc/services/export/repositories/TerritoryRepository.ts";
import {
  TerritoryCodeEnum,
  TerritorySelectorsInterface,
} from "@/pdc/services/territory/contracts/common/interfaces/TerritoryCodeInterface.ts";
export type ResolveParams =
  & { start_at: Date; end_at: Date }
  & Partial<{
    territory_id: number[];
    geo_selector: ExportGeoSelectorInterface;
  }>;
export type ResolveResults = TerritorySelectorsInterface | null;

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
  public displaySelector(_params: ResolveParams): ExportGeoSelectorInterface | null {
    throw new Error("Not implemented");
  }
  public async getTerritoryNames(_geoSelector: ExportGeoSelectorInterface | null): Promise<string[]> {
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
      const ids = params.territory_id || [];
      if (!ids.length) return this.defaultResolveResult;
      return this.resolveTerritories(ids, params);
    }

    const { custom, ...selectors } = params.geo_selector;
    if (!custom?.length) return selectors;

    const customSelectors = await this.resolveTerritories(custom.map(Number), params);
    return this.mergeSelectors([selectors, customSelectors]);
  }

  /**
   * A territory without selectors nor perimeter would export the whole country: refused.
   */
  protected async resolveTerritories(ids: number[], period: ResolveParams): Promise<TerritorySelectorsInterface> {
    const selectors = await Promise.all(ids.map(async (id) => {
      const arr = await this.territoryRepository.getTerritoryPerimeterArr(id, period.start_at, period.end_at);
      const resolved = arr?.length ? { arr } : await this.territoryRepository.getTerritorySelectors(id);
      if (!Object.keys(resolved).length) throw new NotFoundException(`Territory ${id} has no perimeter`);
      return resolved;
    }));
    return this.mergeSelectors(selectors);
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
   * Perimeter as requested, for display: `resolve()` turns territories into their arr,
   * which would list every commune instead of the territory name.
   * A `territory_id` is shown as its territory group (`custom` key).
   */
  public displaySelector(params: ResolveParams): ExportGeoSelectorInterface | null {
    if (params.geo_selector) return params.geo_selector;
    if (params.territory_id?.length) return { custom: params.territory_id.map(String) };
    return null;
  }

  /**
   * Get all territory names from a geo_selector
   *
   * @param geoSelector
   * @returns Array of territory names
   */
  public async getTerritoryNames(geoSelector: ExportGeoSelectorInterface | null): Promise<string[]> {
    if (!geoSelector) return [];

    const { custom, ...selectors } = geoSelector;
    const names: string[] = [];
    const types = Object.keys(selectors) as (keyof TerritorySelectorsInterface)[];

    for (const type of types) {
      const codes = selectors[type];
      if (!codes || !codes.length) continue;

      const batchNames = await this.territoryRepository.getTerritoryNamesBatch(type as string, codes);
      names.push(...batchNames);
    }

    if (custom?.length) {
      names.push(...await this.territoryRepository.getTerritoryGroupNames(custom.map(Number)));
    }

    return names;
  }
}
