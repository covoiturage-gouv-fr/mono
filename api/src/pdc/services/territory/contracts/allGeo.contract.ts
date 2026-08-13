import { PerimeterCode, PerimeterLabel, PerimeterType } from "@/pdc/services/geo/contracts/shared/Perimeter.ts";

export interface SingleResultInterface {
  id: string;
  territory: PerimeterCode;
  l_territory: PerimeterLabel;
  type: PerimeterType;
  year: number;
  is_latest: boolean;
}

export type ResultInterface = SingleResultInterface[];

export const handlerConfig = {
  service: "territory",
  method: "allGeo",
};

export const signature = `${handlerConfig.service}:${handlerConfig.method}` as const;
