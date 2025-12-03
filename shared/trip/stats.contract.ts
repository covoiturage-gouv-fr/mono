import { TripStatInterface } from "./common/interfaces/TripStatInterface";

export interface ParamsInterface extends TripStatInterface {}

interface CommonStatInterface {
  distance: number;
  carpoolers: number;
  trip: number;
  average_carpoolers_by_car: number;
  trip_subsidized: number;
  operators: number;
  incentive_sum: bigint;
  financial_incentive_sum: bigint;
}

export interface SingleResultInterface extends CommonStatInterface {
  month?: number;
  day?: Date;
}

export type ResultInterface = SingleResultInterface[];

export const handlerConfig = {
  service: "trip",
  method: "stats",
} as const;

export const signature = `${handlerConfig.service}:${handlerConfig.method}` as const;
