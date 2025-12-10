export interface UsersInterface {
  meta: {
    page: number;
    total: number;
    totalPages: number;
  };
  data: {
    id?: number;
    firstname: string;
    lastname: string;
    email: string;
    operator_id?: number;
    territory_id?: number;
    phone?: string;
    role: string;
  }[];
}

export interface Company {
  result: {
    data: {
      _id: number;
      siret: string;
    };
  };
}

export enum TerritoryCodeEnum {
  Arr = "arr",
  City = "com",
  CityGroup = "epci",
  Mobility = "aom",
  Region = "reg",
  District = "dep",
}

export interface TerritorySelectorsInterface {
  [TerritoryCodeEnum.Arr]?: string[];
  [TerritoryCodeEnum.City]?: string[];
  [TerritoryCodeEnum.Mobility]?: string[];
  [TerritoryCodeEnum.District]?: string[];
  [TerritoryCodeEnum.CityGroup]?: string[];
  [TerritoryCodeEnum.Region]?: string[];
}

export interface Territory extends Record<string, unknown> {
  _id?: number;
  name: string;
  siret: string;
}

export interface TerritoriesInterface extends Record<string, unknown> {
  meta: {
    pagination: {
      limit: number;
      offset: number;
      total: number;
    };
  };
  data: Territory[];
}

export interface OperatorTokenInterface extends Record<string, unknown> {
  token_id: string;
  operator_id: number;
  role: string;
}

export interface CreateTokenResponseInterface {
  access_key: string;
  secret_key: string;
}

export interface OperatorsInterface {
  meta: {
    page: number;
    total: number;
    totalPages: number;
  };
  data: {
    id?: number;
    name: string;
    siret: string;
  }[];
}

export interface CampaignsInterface {
  _id: string;
  start_date: Date;
  end_date: Date;
  territory_id: string;
  territory_name: string;
  name: string;
  description: string;
  unit: string;
  status: string;
  handler: string;
  incentive_sum: number;
  max_amount: number;
  territory_selector: TerritorySelectorsInterface;
}

export interface CampaignsListInterface {
  meta: {
    page: number;
    total: number;
    totalPages: number;
  };
  data: CampaignsInterface[];
}
