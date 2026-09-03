import Chart from "@/components/observatoire/charts/Chart";
import { chartLabels } from "@/helpers/graph";
import { useObservatoryData } from "@/hooks/useObservatoryData";

export default function DistanceGraph({ title }: { title: string }) {
  const { data, error, loading, period } = useObservatoryData<
    Record<string, number>[]
  >("evol-flux", ["indic=distance"]);

  // Distance moyenne par trajet = distance totale / nombre de trajets.
  const values = (data ?? []).map((d) => d.distance / d.journeys).reverse();

  return (
    <Chart
      kind="line"
      title={title}
      labels={chartLabels(data ?? [], period)}
      data={values}
      unit="km"
      download={{ data: data ?? [], filename: "distance" }}
      loading={loading}
      error={error}
    />
  );
}
