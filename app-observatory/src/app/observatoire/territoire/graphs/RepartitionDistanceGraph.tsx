import DownloadButton from "@/components/observatoire/DownloadButton";
import { GetApiUrl } from "@/helpers/api";
import { useApi } from "@/hooks/useApi";
import { DistributionDistanceDataInterface } from "@/interfaces/observatoire/dataInterfaces";
import { fr } from "@codegouvfr/react-dsfr";
//import { ArcElement, ChartData, Chart as ChartJS, Legend, Title, Tooltip } from 'chart.js';
import {
  BarElement,
  CategoryScale,
  ChartData,
  Chart as ChartJS,
  Legend,
  LinearScale,
  Title,
  Tooltip,
} from "chart.js";
import ChartDataLabels, { Context } from "chartjs-plugin-datalabels";
import { Bar } from "react-chartjs-2";
import { useDashboardContext } from "../../../../context/DashboardProvider";

ChartJS.register(
  BarElement,
  CategoryScale,
  LinearScale,
  Title,
  Tooltip,
  Legend
);

export default function RepartitionDistanceGraph({ title }: { title: string }) {
  const { dashboard } = useDashboardContext();
  const options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        display: false,
      },
    },
  };
  const params = [
    `code=${dashboard.params.code}`,
    `type=${dashboard.params.type}`,
    `year=${dashboard.params.year}`,
    `direction=both`,
  ];
  const url = GetApiUrl("journeys-by-distances", params);
  const { data, error, loading } =
    useApi<DistributionDistanceDataInterface[]>(url);
  const plugins: any = [ChartDataLabels];
  const dataset = data
    ?.find((d) => d.direction === "both")
    ?.distances.map((d) => d.journeys);
  const datasetSum = (dataset: number[]) => {
    let sum = 0;
    dataset.map((d) => {
      sum += d;
    });
    return sum;
  };
  const chartData = () => {
    const labels = [
      "< 10 km",
      "10-20 km",
      "20-30 km",
      "30-40 km",
      "40-50 km",
      "> 50 km",
    ];
    const datasets = [
      {
        label: "trajets",
        data: dataset,
        backgroundColor: ["#3182bd"],
        datalabels: {
          labels: {
            value: {
              align: "bottom",
              color: "black",
              font: (context: Context) => {
                const avgSize = Math.round(
                  (context.chart.height + context.chart.width) / 2
                );
                const params = {
                  size:
                    Math.round(avgSize / 24) > 10
                      ? 12
                      : Math.round(avgSize / 24),
                };
                return params;
              },
              formatter: (value: number, ctx: Context) => {
                let sum = 0;
                const dataArr = ctx.chart.data.datasets[0].data as number[];
                dataArr.map((data: number) => {
                  sum += data;
                });
                const percentage = ((value * 100) / sum).toFixed(1) + "%";
                return percentage;
              },
            },
          },
        },
      },
    ];
    return { labels: labels, datasets: datasets };
  };

  return (
    <>
      {loading && (
        <div className={fr.cx("fr-callout")}>
          <h3 className={fr.cx("fr-callout__title")}>{title}</h3>
          <div>Chargement en cours...</div>
        </div>
      )}
      {error && (
        <div className={fr.cx("fr-callout")}>
          <h3 className={fr.cx("fr-callout__title")}>{title}</h3>
          <div>{`Un problème est survenu au chargement des données: ${error}`}</div>
        </div>
      )}
      {!data ||
        (data.length == 0 && (
          <div className={fr.cx("fr-callout")}>
            <h3 className={fr.cx("fr-callout__title")}>{title}</h3>
            <div>Pas de données disponibles pour ce graphique...</div>
          </div>
        ))}
      {!loading && !error && data && data.length > 0 && (
        <div className={fr.cx("fr-callout")}>
          <div className={fr.cx("fr-callout__title", "fr-text--xl")}>
            {title}
          </div>
          <DownloadButton
            title={"Télécharger les données du graphique"}
            data={data!}
            filename="repartition_distance"
          />
          <figure
            className="graph-wrapper"
            style={{ backgroundColor: "#fff", height: "350px" }}
          >
            <Bar
              options={options}
              plugins={plugins}
              data={chartData() as ChartData<"bar", number[]>}
              aria-hidden
            />
            {chartData() && (
              <figcaption className={fr.cx("fr-sr-only")}>
                {dataset && (
                  <>
                    <p>
                      {
                        "Données de répartition des trajets (tout sens confondus)"
                      }
                    </p>
                    <ul>
                      {dataset.map((d, i) => {
                        return (
                          <li key={i}>
                            {chartData().labels[i]} :{" "}
                            {((d * 100) / datasetSum(dataset)).toFixed(1) + "%"}
                          </li>
                        );
                      })}
                    </ul>
                  </>
                )}
              </figcaption>
            )}
          </figure>
        </div>
      )}
    </>
  );
}
