import { TripStatInterface } from "./common/interfaces/TripStatInterface";

export interface ParamsInterface extends TripStatInterface {}

interface CommonStatInterface {
  incentive_sum: bigint;
  financial_incentive_sum: bigint;
}

interface StatByMonthInterface extends CommonStatInterface {
  month: number;
}
interface StatByDayInterface extends CommonStatInterface {
  day: Date;
}

export type SingleResultInterface = StatByDayInterface | StatByMonthInterface;

export type ResultInterface = SingleResultInterface[];

export const handlerConfig = {
  service: "trip",
  method: "financialStats",
} as const;

export const signature = `${handlerConfig.service}:${handlerConfig.method}` as const;
