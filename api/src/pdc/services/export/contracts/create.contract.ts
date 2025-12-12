import { Timezone } from "@/pdc/providers/validator/types.ts";
import { ExportStatus, ExportTarget } from "@/pdc/services/export/models/Export.ts";
import { TerritorySelectorsInterface } from "@/pdc/services/territory/contracts/common/interfaces/TerritoryCodeInterface.ts";

export type ParamsInterface = {
  tz: Timezone;
  start_at: Date;
  end_at: Date;
  created_by: number;
  recipients?: string[];
  operator_id?: number[];
  geo_selector?: TerritorySelectorsInterface;
  territory_id?: number[];
};

export type ResultInterface = {
  uuid: string;
  target: ExportTarget;
  status: ExportStatus;
  start_at: Date;
  end_at: Date;
};

export const handlerConfig = {
  service: "export",
  method: "createVersionThree",
} as const;

export const signatureV3 = `${handlerConfig.service}:${handlerConfig.method}` as const;
