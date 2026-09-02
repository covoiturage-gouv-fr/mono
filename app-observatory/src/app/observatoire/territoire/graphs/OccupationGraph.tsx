import Chart from "@/components/observatoire/charts/Chart";
import { chartLabels } from "@/helpers/graph";
import { useObservatoryData } from "@/hooks/useObservatoryData";
import { OccupationIndicators } from "@/interfaces/observatoire/componentsInterfaces";

export default function OccupationGraph({
  title,
  indic,
}: {
  title: string;
  indic: OccupationIndicators;
}) {
  const { data, error, loading, period } = useObservatoryData<
    Record<string, number>[]
  >("evol-occupation", [`indic=${indic}`]);

  const values = (data ?? []).map((d) => d[indic]).reverse();

  return (
    <Chart
      kind="line"
      title={title}
      labels={chartLabels(data ?? [], period)}
      data={values}
      download={{ data: data ?? [], filename: "occupation" }}
      loading={loading}
      error={error}
    />
  );
}
