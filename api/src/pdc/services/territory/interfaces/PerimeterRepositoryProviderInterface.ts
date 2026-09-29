import { TerritorySelectorsInterface } from "../contracts/common/interfaces/TerritoryCodeInterface.ts";
import { TerritoryPerimeterInterface } from "../contracts/common/interfaces/TerritoryPerimeterInterface.ts";

export interface ArrDescriptionInterface {
  arr: string;
  label: string;
  pop: number | null;
}

export interface ComEvolutionInterface {
  old_com: string;
  new_com: string;
}

export interface OwnedPolicyInterface {
  _id: number;
  name: string;
  status: string;
}

export abstract class PerimeterRepositoryProviderInterfaceResolver {
  abstract findTerritory(territory_id: number): Promise<{ _id: number; name: string } | undefined>;

  abstract findTerritoryByName(name: string): Promise<{ _id: number; name: string } | undefined>;

  abstract createTerritory(
    name: string,
    siret: string | undefined,
    data: Omit<TerritoryPerimeterInterface, "version">,
  ): Promise<number>;

  abstract findTerritoriesWithVersions(): Promise<number[]>;

  abstract findByTerritory(territory_id: number): Promise<TerritoryPerimeterInterface[]>;

  abstract create(
    territory_id: number,
    data: Omit<TerritoryPerimeterInterface, "version">,
  ): Promise<TerritoryPerimeterInterface>;

  abstract getArr(territory_id: number, at: Date): Promise<string[]>;

  abstract resolve(selectors: TerritorySelectorsInterface): Promise<{ arr: string[]; unknown: string[] }>;

  abstract describe(arr: string[]): Promise<ArrDescriptionInterface[]>;

  abstract findEvolutions(): Promise<ComEvolutionInterface[]>;

  abstract findPolicies(territory_id: number): Promise<OwnedPolicyInterface[]>;
}
