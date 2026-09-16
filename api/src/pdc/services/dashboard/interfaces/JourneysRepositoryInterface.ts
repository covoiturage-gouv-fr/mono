import type { ResultInterface as JourneysIncentiveByDayResultInterface } from "@/pdc/services/dashboard/actions/JourneysIncentiveByDayAction.ts";
import type { ResultInterface as JourneysIncentiveByMonthResultInterface } from "@/pdc/services/dashboard/actions/JourneysIncentiveByMonthAction.ts";
import {
  JourneysByDay as JourneysIncentiveByDayParamsInterface,
  JourneysByDay as JourneysOperatorsByDayParamsInterface,
  JourneysByMonth as JourneysIncentiveByMonthParamsInterface,
  JourneysByMonth as JourneysOperatorsByMonthParamsInterface,
} from "@/pdc/services/dashboard/dto/Journeys.ts";
import type { ResultInterface as JourneysOperatorsByDayResultInterface } from "../actions/JourneysOperatorsByDayAction.ts";
import type { ResultInterface as JourneysOperatorsByMonthResultInterface } from "../actions/JourneysOperatorsByMonthAction.ts";

// Périmètre de l'appelant, recopié depuis la session par les middlewares (jamais depuis le corps).
export type CallerScope = { territory_id?: number; operator_id?: number };

export type {
  JourneysIncentiveByDayParamsInterface,
  JourneysIncentiveByDayResultInterface,
  JourneysIncentiveByMonthParamsInterface,
  JourneysIncentiveByMonthResultInterface,
  JourneysOperatorsByDayParamsInterface,
  JourneysOperatorsByDayResultInterface,
  JourneysOperatorsByMonthParamsInterface,
  JourneysOperatorsByMonthResultInterface,
};

export interface JourneysRepositoryInterface {
  getOperatorsByMonth(
    params: JourneysOperatorsByMonthParamsInterface & CallerScope,
  ): Promise<JourneysOperatorsByMonthResultInterface[]>;
  getOperatorsByDay(
    params: JourneysOperatorsByDayParamsInterface & CallerScope,
  ): Promise<JourneysOperatorsByDayResultInterface[]>;
  getIncentiveByMonth(
    params: JourneysIncentiveByMonthParamsInterface & CallerScope,
  ): Promise<JourneysIncentiveByMonthResultInterface[]>;
  getIncentiveByDay(
    params: JourneysIncentiveByDayParamsInterface & CallerScope,
  ): Promise<JourneysIncentiveByDayResultInterface[]>;
}

export abstract class JourneysRepositoryInterfaceResolver implements JourneysRepositoryInterface {
  async getOperatorsByMonth(
    params: JourneysOperatorsByMonthParamsInterface & CallerScope,
  ): Promise<JourneysOperatorsByMonthResultInterface[]> {
    throw new Error();
  }
  async getOperatorsByDay(
    params: JourneysOperatorsByDayParamsInterface & CallerScope,
  ): Promise<JourneysOperatorsByDayResultInterface[]> {
    throw new Error();
  }
  async getIncentiveByMonth(
    params: JourneysIncentiveByMonthParamsInterface & CallerScope,
  ): Promise<JourneysIncentiveByMonthResultInterface[]> {
    throw new Error();
  }
  async getIncentiveByDay(
    params: JourneysIncentiveByDayParamsInterface & CallerScope,
  ): Promise<JourneysIncentiveByDayResultInterface[]> {
    throw new Error();
  }
}
