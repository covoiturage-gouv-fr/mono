export enum TerritoryCodeEnum {
  Arr = "arr",
  City = "com",
  CityGroup = "epci",
  Mobility = "aom",
  Id = "_id",
}

export interface TerritorySelectorsInterface {
  [TerritoryCodeEnum.Arr]?: string[];
  [TerritoryCodeEnum.City]?: string[];
  [TerritoryCodeEnum.Mobility]?: string[];
  [TerritoryCodeEnum.Id]?: number[];
  [TerritoryCodeEnum.CityGroup]?: string[];
}

export interface TerritoryGroupInterface {
  _id: number;
  company_id: number;
  name: string;
  shortname: string;
  contacts;
  address: {
    street: string;
    postcode: string;
    cedex?: string;
    city: string;
    country: string;
  };
  created_at: Date;
  updated_at: Date;
  deleted_at?: Date;
  selector: TerritorySelectorsInterface;
}

export type CreateTerritoryGroupInterface = Omit<
  TerritoryGroupInterface,
  "created_at" | "updated_at" | "deleted_at" | "shortname"
>;

export const idfm: CreateTerritoryGroupInterface = {
  _id: 1,
  company_id: 1,
  name: "Ile-De-France-Mobilité",
  contacts: {},
  address: {
    street: "39 bis-41 rue de Châteaudun",
    postcode: "75009 Paris",
    cedex: "Paris",
    city: "Paris",
    country: "France",
  },
  selector: { aom: ["217500016"] },
};

export const lyon: CreateTerritoryGroupInterface = {
  _id: 2,
  company_id: 2,
  name: "Metropole de Lyon",
  contacts: {},
  address: {
    street: "20 rue du Lac",
    postcode: "69003 Lyon",
    city: "Lyon",
    country: "France",
  },
  selector: { aom: ["200046977"] },
};

export const territory_groups: CreateTerritoryGroupInterface[] = [idfm, lyon];
