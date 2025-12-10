import { PeriodType } from "../interfaces/observatoire/componentsInterfaces";
import { monthList, semesterList, trimesterList, yearList } from "./lists";

const getPeriodName = (
  periodList: { id: number; name: string }[],
  id: number,
): string => {
  const period = periodList.find((l) => l.id == id);
  return period ? period.name : "";
};

export const chartLabels = (
  data: Record<string, number>[],
  period: PeriodType,
): string[] => {
  const labels: string[] = [];
  switch (period) {
    case "month":
      data.map((d) => {
        labels.push(`${getPeriodName(monthList, d.month)} ${d.year}`);
      });
      break;
    case "trimester":
      data.map((d) => {
        labels.push(`${getPeriodName(trimesterList, d.trimester)} ${d.year}`);
      });
      break;
    case "semester":
      data.map((d) => {
        labels.push(`${getPeriodName(semesterList, d.semester)} ${d.year}`);
      });
      break;
    case "year":
      data.map((d) => {
        labels.push(`${getPeriodName(yearList, d.year)}`);
      });
      break;
  }
  return labels.reverse();
};
