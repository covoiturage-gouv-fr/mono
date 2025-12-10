import { useDashboardContext } from "../context/DashboardProvider";

export function GetPeriod() {
  const { dashboard } = useDashboardContext();
  const period = {
    start_date: new Date(dashboard.params.year, dashboard.params.month - 1, 2)
      .toISOString().slice(0, 10),
    end_date: new Date(dashboard.params.year, dashboard.params.month)
      .toISOString().slice(0, 10),
  };
  switch (dashboard.params.period) {
    case "trimester":
      const startTrimesterMonths = [0, 3, 6, 9]; // Mois de début des trimestres (janvier, avril, juillet, octobre)
      const endTrimesterMonths = [2, 5, 8, 11]; // Mois de fin des trimestres (mars, juin, septembre, décembre)
      period.start_date = new Date(
        dashboard.params.year,
        startTrimesterMonths[dashboard.params.trimester - 1],
        2,
      ).toISOString().slice(0, 10); // Premier jour du mois du trimestre
      period.end_date = new Date(
        dashboard.params.year,
        endTrimesterMonths[dashboard.params.trimester - 1] + 1,
      ).toISOString().slice(0, 10); // Dernier jour du mois du trimestre
      break;
    case "semester":
      const startSemesterMonths = [0, 6]; // Mois de début des semestres (janvier, juillet)
      const endSemesterMonths = [5, 11]; // Mois de fin des trimestres (juin, décembre)
      period.start_date = new Date(
        dashboard.params.year,
        startSemesterMonths[dashboard.params.semester - 1],
        2,
      ).toISOString().slice(0, 10); // Premier jour du mois du trimestre
      period.end_date = new Date(
        dashboard.params.year,
        endSemesterMonths[dashboard.params.semester - 1] + 1,
      ).toISOString().slice(0, 10); // Dernier jour du mois du trimestre
      break;
    case "year":
      period.start_date = new Date(
        dashboard.params.year,
        0,
        2,
      ).toISOString().slice(0, 10); // Premier jour de l'année
      period.end_date = new Date(
        dashboard.params.year,
        12,
        1,
      ).toISOString().slice(0, 10); // Dernier jour du mois du trimestre
      break;
  }
  return period;
}
