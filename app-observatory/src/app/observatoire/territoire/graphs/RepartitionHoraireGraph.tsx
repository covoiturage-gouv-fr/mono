import Chart, { ChartSeries } from "@/components/observatoire/charts/Chart";
import { useObservatoryData } from "@/hooks/useObservatoryData";
import { Hour } from "@/interfaces/observatoire/Perimeter";
import { DistributionHoraireDataInterface } from "@/interfaces/observatoire/dataInterfaces";

const LABELS = Array.from({ length: 24 }, (_, h) => `${h}h`);

// Complète la série sur les 24 heures (0 quand l'heure est absente).
const fillHours = (hours: Hour[]): number[] => {
  const byHour = new Map(hours.map((h) => [h.hour, h.journeys]));
  return Array.from({ length: 24 }, (_, h) => byHour.get(h) ?? 0);
};

export default function RepartitionHoraireGraph({ title }: { title: string }) {
  const { data, error, loading, type } = useObservatoryData<
    DistributionHoraireDataInterface[]
  >("journeys-by-hours");

  const hoursFor = (direction: string) =>
    fillHours(data?.find((d) => d.direction === direction)?.hours ?? []);

  const series: ChartSeries[] = !data?.length
    ? []
    : type === "country"
      ? [
          {
            label: "Tout sens confondus",
            data: hoursFor("both"),
            color: "rgba(106, 106, 244, 0.8)",
          },
        ]
      : [
          {
            label: "Origine",
            data: hoursFor("from"),
            color: "rgba(106, 106, 244, 0.8)",
          },
          {
            label: "Destination",
            data: hoursFor("to"),
            color: "rgba(183, 167, 63, 0.8)",
          },
        ];

  return (
    <Chart
      kind="bar"
      title={title}
      labels={LABELS}
      data={series}
      unit="trajets"
      height={350}
      legend={type !== "country"}
      srIntro={series.map((s) => `Répartition horaire — ${s.label.toLowerCase()}`)}
      download={{ data: data ?? [], filename: "repartition_horaire" }}
      loading={loading}
      error={error}
    />
  );
}
