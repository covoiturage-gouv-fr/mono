import { INSEECode, PerimeterType } from "@/interfaces/observatoire/Perimeter";
import { PeriodType } from "../observatoire/componentsInterfaces";
import { TerritoryListInterface } from "../observatoire/dataInterfaces";

export type Params = {
  code: INSEECode;
  type: PerimeterType;
  observe: PerimeterType;
  name: string;
};

export interface DashboardParams extends Params {
  period: PeriodType;
  year: number;
  month: number;
  trimester: number;
  semester: number;
  map: number;
  graph: number;
}

export type DashboardContextType = {
  dashboard: {
    params: DashboardParams;
    lastPeriod: number;
    loading: boolean;
    getParams: (params: Params) => void;
    onLoadTerritory: (value?: { code: INSEECode; type: PerimeterType }) => void;
    onChangeTerritory: (value: TerritoryListInterface) => void;
    getName: (
      value: { code: INSEECode; type: PerimeterType },
    ) => Promise<string>;
    onChangePeriod: (value: PeriodType) => void;
    onChangeMonth: (value: number) => void;
    onChangeTrimester: (value: number) => void;
    onChangeSemester: (value: number) => void;
    onChangeYear: (value: number) => void;
    onChangeObserve: (value: PerimeterType) => void;
    onChangeGraph: (value: number) => void;
    onChangeMap: (value: number) => void;
  };
};
