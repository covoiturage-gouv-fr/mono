import { TerritorySelectorsInterface } from "@/pdc/services/territory/contracts/common/interfaces/TerritoryCodeInterface.ts";

export interface PolicyTerritoryInterface {
  version: number;
  arr: string[];
  valid_from: Date;
  valid_to: Date | null;
}

export interface ArrDescriptionInterface {
  arr: string;
  label: string;
  pop: number | null;
}

export interface ComEvolutionInterface {
  old_com: string;
  new_com: string;
}

export abstract class PolicyTerritoryRepositoryProviderInterfaceResolver {
  abstract findByPolicy(policy_id: number): Promise<PolicyTerritoryInterface[]>;

  abstract create(
    policy_id: number,
    data: Omit<PolicyTerritoryInterface, "version">,
  ): Promise<PolicyTerritoryInterface>;

  abstract resolve(selectors: TerritorySelectorsInterface): Promise<{ arr: string[]; unknown: string[] }>;

  abstract describe(arr: string[]): Promise<ArrDescriptionInterface[]>;

  abstract findEvolutions(): Promise<ComEvolutionInterface[]>;
}
