"use client";
import { INSEECode, PerimeterType } from "@/interfaces/observatoire/Perimeter";
import { TerritoryListInterface } from "@/interfaces/observatoire/dataInterfaces";
import { useCallback, useState } from "react";
import { fetchSearchAPI } from "../helpers/search";
import { Params } from "../interfaces/common/contextInterface";
import { PeriodType } from "../interfaces/observatoire/componentsInterfaces";

export const useDashboard = () => {
  const lastDataDate = () => {
    const now = new Date();
    const lastWeek = new Date(now.setDate(now.getDate() - 7));
    return new Date(lastWeek.setMonth(lastWeek.getMonth()));
  };
  const month = new Date(lastDataDate()).getMonth() > 0 ? new Date(lastDataDate()).getMonth() : 12;
  const year = new Date(lastDataDate()).getMonth() > 0
    ? new Date(lastDataDate()).getFullYear()
    : new Date(lastDataDate()).getFullYear() - 1;
  const [params, setParams] = useState({
    code: "XXXXX",
    name: "France",
    type: "country" as PerimeterType,
    observe: "com" as PerimeterType,
    year: year,
    month: month,
    period: "month" as PeriodType,
    trimester: Math.ceil(month / 3),
    semester: Math.ceil(month / 6),
    map: 1,
    graph: 1,
  });
  const [lastPeriod] = useState(new Date(lastDataDate()).getTime());
  const [loading, setLoading] = useState(true);

  const getParams = useCallback((params: Params) => {
    setParams((p) => {
      return { ...p, ...params };
    });
  }, []);

  const onLoadTerritory = useCallback(
    async (value?: { code: INSEECode; type: PerimeterType }) => {
      setLoading(true);
      const params = value ? value : { code: "XXXXX", type: "country" as PerimeterType };
      const name = await getName(params);
      setParams((p) => {
        return { ...p, ...params, name: name, observe: "com" } as typeof p;
      });
      setLoading(false);
    },
    [],
  );

  const onChangeTerritory = useCallback((value: TerritoryListInterface) => {
    setLoading(true);
    setParams((p) => {
      const params = { code: "XXXXX", name: "France", type: "country" };
      if (value) {
        params.code = value.territory;
        params.name = value.l_territory;
        params.type = value.type;
      }
      return { ...p, ...params, observe: "com" } as typeof p;
    });
    setLoading(false);
  }, []);

  const onChangePeriod = useCallback((value: PeriodType) => {
    setLoading(true);
    setParams((p) => {
      return { ...p, period: value };
    });
    setLoading(false);
  }, []);
  const onChangeMonth = useCallback((value: number) => {
    setParams((p) => {
      return { ...p, month: value };
    });
  }, []);
  const onChangeTrimester = useCallback((value: number) => {
    setParams((p) => {
      return { ...p, trimester: value };
    });
  }, []);
  const onChangeSemester = useCallback((value: number) => {
    setParams((p) => {
      return { ...p, semester: value };
    });
  }, []);
  const onChangeYear = useCallback((value: number) => {
    setParams((p) => {
      return { ...p, year: value };
    });
  }, []);
  const onChangeObserve = useCallback((value: PerimeterType) => {
    setParams((p) => {
      return { ...p, observe: value };
    });
  }, []);
  const onChangeGraph = useCallback((value: number) => {
    setParams((p) => {
      return { ...p, graph: value };
    });
  }, []);
  const onChangeMap = useCallback((value: number) => {
    setParams((p) => {
      return { ...p, map: value };
    });
  }, []);

  const getName = async (value: { code: INSEECode; type: PerimeterType }) => {
    const query = {
      q: `${value.code}_${value.type}`,
      attributesToSearchOn: ["id"],
      limit: 1,
    };
    const response = await fetchSearchAPI("indexes/geo/search", {
      method: "post",
      body: JSON.stringify(query),
    });
    return response ? response.hits[0].l_territory as string : "France";
  };

  return {
    params,
    lastPeriod,
    loading,
    getParams,
    onLoadTerritory,
    onChangeTerritory,
    getName,
    onChangePeriod,
    onChangeMonth,
    onChangeTrimester,
    onChangeSemester,
    onChangeYear,
    onChangeObserve,
    onChangeGraph,
    onChangeMap,
  };
};
