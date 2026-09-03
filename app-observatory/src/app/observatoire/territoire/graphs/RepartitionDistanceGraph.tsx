import Chart from "@/components/observatoire/charts/Chart";
import { useObservatoryData } from "@/hooks/useObservatoryData";
import { DistributionDistanceDataInterface } from "@/interfaces/observatoire/dataInterfaces";
import { Context } from "chartjs-plugin-datalabels";

const LABELS = [
  "< 10 km",
  "10-20 km",
  "20-30 km",
  "30-40 km",
  "40-50 km",
  "> 50 km",
];

const sum = (values: number[]) => values.reduce((acc, v) => acc + v, 0);

export default function RepartitionDistanceGraph({ title }: { title: string }) {
  const { data, error, loading } = useObservatoryData<
    DistributionDistanceDataInterface[]
  >("journeys-by-distances", ["direction=both"]);

  const values =
    data?.find((d) => d.direction === "both")?.distances.map((d) => d.journeys) ??
    [];
  const total = sum(values);
  const pct = (value: number) => `${((value * 100) / total).toFixed(1)} %`;

  return (
    <Chart
      kind="bar"
      title={title}
      labels={LABELS}
      data={[
        {
          label: "trajets",
          data: values,
          color: "#3182bd",
          datalabels: {
            labels: {
              value: {
                align: "bottom",
                color: "black",
                font: (context: Context) => {
                  const avgSize = Math.round(
                    (context.chart.height + context.chart.width) / 2,
                  );
                  return {
                    size:
                      Math.round(avgSize / 24) > 10
                        ? 12
                        : Math.round(avgSize / 24),
                  };
                },
                formatter: (value: number) => pct(value),
              },
            },
          },
        },
      ]}
      formatValue={pct}
      height={350}
      srIntro="Répartition des trajets par distance (tout sens confondus)"
      download={{ data: data ?? [], filename: "repartition_distance" }}
      loading={loading}
      error={error}
    />
  );
}
