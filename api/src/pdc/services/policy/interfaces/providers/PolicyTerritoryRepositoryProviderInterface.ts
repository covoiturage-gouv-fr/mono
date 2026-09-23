export const TERRITORY_TYPES = ["arr", "com", "epci", "aom", "dep", "reg", "country"] as const;
export type TerritoryType = typeof TERRITORY_TYPES[number];

export interface TerritoryCode {
  type: TerritoryType;
  code: string;
}

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

export abstract class PolicyTerritoryRepositoryProviderInterfaceResolver {
  abstract findByPolicy(policy_id: number): Promise<PolicyTerritoryInterface[]>;

  abstract create(
    policy_id: number,
    data: Omit<PolicyTerritoryInterface, "version">,
  ): Promise<PolicyTerritoryInterface>;

  abstract resolve(
    codes: TerritoryCode[],
    fromYear: number,
  ): Promise<{ arr: string[]; unknown: TerritoryCode[] }>;

  abstract describe(arr: string[]): Promise<ArrDescriptionInterface[]>;
}
