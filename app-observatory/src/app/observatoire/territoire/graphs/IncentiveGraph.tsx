import Chart from "@/components/observatoire/charts/Chart";
import { useObservatoryData } from "@/hooks/useObservatoryData";
import { IncentiveDataInterface } from "@/interfaces/observatoire/dataInterfaces";
import { Context } from "chartjs-plugin-datalabels";

const LABELS = ["Collectivité(s)", "Opérateur(s)", "Autres", "Pas d'incitation"];
const COLORS = ["#3182bd", "#6baed6", "#9ecae1", "#fcbfbf"];

const responsiveSize =
  (max: number, weight?: string) => (context: Context) => {
    const avgSize = Math.round(
      (context.chart.height + context.chart.width) / 2,
    );
    const size = Math.round(avgSize / 24) > 10 ? max : Math.round(avgSize / 24);
    return weight ? { size, weight } : { size };
  };

const sum = (values: number[]) => values.reduce((acc, v) => acc + v, 0);

export default function IncentiveGraph({ title }: { title: string }) {
  const { data, error, loading } = useObservatoryData<IncentiveDataInterface[]>(
    "incentive",
    ["direction=both"],
  );

  const row = data?.[0];
  const values = row
    ? [row.collectivite, row.operateur, row.autres, row.no_incentive]
    : [];
  const total = sum(values);
  const pct = (value: number) => `${((value * 100) / total).toFixed(1)} %`;

  return (
    <Chart
      kind="doughnut"
      title={title}
      labels={LABELS}
      data={[
        {
          label: "incitations",
          data: values,
          colors: COLORS,
          datalabels: {
            labels: {
              name: {
                align: "middle",
                color: "black",
                font: responsiveSize(14, "bold"),
                formatter: (_value: number, ctx: Context) =>
                  ctx.chart.data.labels
                    ? ctx.chart.data.labels[ctx.dataIndex]
                    : "",
              },
              value: {
                align: "bottom",
                color: "black",
                font: responsiveSize(12),
                formatter: (value: number) => pct(value),
              },
            },
          },
        },
      ]}
      formatValue={pct}
      height={350}
      srIntro="Répartition des incitations par type d'incitateur (tout sens confondus)"
      download={{ data: data ?? [], filename: "incentive" }}
      loading={loading}
      error={error}
    />
  );
}
