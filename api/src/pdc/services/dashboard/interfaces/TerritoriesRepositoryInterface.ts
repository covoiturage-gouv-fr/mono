export interface TerritoryWithSiret {
  id: number;
  name: string;
  siret: string;
}

export interface TerritoriesRepositoryInterface {
  getTerritoryById(id: number): Promise<TerritoryWithSiret | null>;
}

export abstract class TerritoriesRepositoryInterfaceResolver implements TerritoriesRepositoryInterface {
  async getTerritoryById(id: number): Promise<TerritoryWithSiret | null> {
    throw new Error("Not implemented");
  }
}
