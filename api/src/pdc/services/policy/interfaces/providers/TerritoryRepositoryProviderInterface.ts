import {
  TerritoryCodeInterface,
  TerritorySelectorsInterface,
} from "../index.ts";
import { TerritoryPerimeterInterface } from "@/pdc/services/territory/contracts/common/interfaces/TerritoryPerimeterInterface.ts";
export interface TerritoryRepositoryProviderInterface {
  findByPoint(
    { lon, lat }: { lon: number; lat: number },
  ): Promise<TerritoryCodeInterface>;
  findUUIDById(
    _id: number | number[],
  ): Promise<{ _id: number; uuid: string }[]>;
  findUUIDByOperatorId(_id: number): Promise<string>;
  findBySelector(data: Partial<TerritoryCodeInterface>): Promise<number[]>;
  findSelectorFromId(id: number): Promise<TerritorySelectorsInterface>;
  findPerimeters(territory_id: number): Promise<TerritoryPerimeterInterface[]>;
}
export abstract class TerritoryRepositoryProviderInterfaceResolver
  implements TerritoryRepositoryProviderInterface {
  async findByPoint(
    { lon, lat }: { lon: number; lat: number },
  ): Promise<TerritoryCodeInterface> {
    throw new Error();
  }
  async findUUIDById(
    _id: number | number[],
  ): Promise<{ _id: number; uuid: string }[]> {
    throw new Error();
  }
  async findUUIDByOperatorId(_id: number): Promise<string> {
    throw new Error();
  }
  async findBySelector(
    data: Partial<TerritoryCodeInterface>,
  ): Promise<number[]> {
    throw new Error();
  }
  async findSelectorFromId(id: number): Promise<TerritorySelectorsInterface> {
    throw new Error();
  }
  async findPerimeters(territory_id: number): Promise<TerritoryPerimeterInterface[]> {
    throw new Error();
  }
}
