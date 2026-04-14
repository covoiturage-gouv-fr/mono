import type {
  ResultInterface as LastRecordResultInterface,
} from "@/pdc/services/observatory/actions/LastRecordAction.ts";
import type { LastRecord as LastRecordParamsInterface } from "@/pdc/services/observatory/dto/LastRecord.ts";

export type { LastRecordParamsInterface, LastRecordResultInterface };

export interface LastRecordRepositoryInterface {
  getLastRecord(
    params: LastRecordParamsInterface,
    maxYear?: number,
    maxMonth?: number,
  ): Promise<LastRecordResultInterface>;
}

export abstract class LastRecordRepositoryInterfaceResolver implements LastRecordRepositoryInterface {
  async getLastRecord(
    params: LastRecordParamsInterface,
    maxYear?: number,
    maxMonth?: number,
  ): Promise<LastRecordResultInterface> {
    throw new Error();
  }
}
