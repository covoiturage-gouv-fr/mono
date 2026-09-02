import Chart from "@/components/observatoire/charts/Chart";
import { chartLabels } from "@/helpers/graph";
import { useObservatoryData } from "@/hooks/useObservatoryData";
import { FluxIndicators } from "@/interfaces/observatoire/componentsInterfaces";

export default function FluxGraph({
  title,
  indic,
}: {
  title: string;
  indic: FluxIndicators;
}) {
  const { data, error, loading, period } = useObservatoryData<
    Record<string, number>[]
  >("evol-flux", [`indic=${indic}`]);

  // On écarte le dernier point (mois en cours, incomplet).
  const values = (data ?? [])
    .map((d) => d[indic])
    .reverse()
    .slice(0, -1);

  return (
    <Chart
      kind="line"
      title={title}
      labels={chartLabels(data ?? [], period).slice(0, -1)}
      data={values}
      unit="trajets"
      download={{ data: data ?? [], filename: "flux" }}
      loading={loading}
      error={error}
    />
  );
}
