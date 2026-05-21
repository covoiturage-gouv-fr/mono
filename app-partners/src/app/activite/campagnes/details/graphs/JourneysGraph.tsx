import { useIncentiveGraph } from "@/hooks/api";
import { fr } from "@codegouvfr/react-dsfr";
import { Alert } from "@codegouvfr/react-dsfr/Alert";
import { Tag } from "@codegouvfr/react-dsfr/Tag";
import {
  CategoryScale,
  Chart as ChartJS,
  Filler,
  Legend,
  LineElement,
  LinearScale,
  PointElement,
  Title,
  Tooltip,
} from "chart.js";
import { useState } from "react";
import { Line } from "react-chartjs-2";

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Title, Tooltip, Legend, Filler);

export default function JourneysGraph(props: { title: string; campaignId: number }) {
  const [period, setPeriod] = useState<"month" | "day">("month");
  const { data, error, loading } = useIncentiveGraph({ campaign_id: props.campaignId, period });

  const header = (
    <>
      <h3 className={fr.cx("fr-callout__title")}>{props.title}</h3>
      <ul className={fr.cx("fr-tags-group")}>
        <li>
          <Tag
            nativeButtonProps={{ onClick: () => setPeriod("month") }}
            pressed={period === "month"}
          >
            Evolution mensuelle
          </Tag>
        </li>
        <li>
          <Tag
            nativeButtonProps={{ onClick: () => setPeriod("day") }}
            pressed={period === "day"}
          >
            Evolution journalière
          </Tag>
        </li>
      </ul>
    </>
  );

  if (error) {
    return (
      <div className={fr.cx("fr-my-4w")}>
        {header}
        <Alert
          severity="error"
          title="Erreur de chargement du graphique"
          description={error.message}
        />
      </div>
    );
  }

  if (loading) {
    return (
      <div className={fr.cx("fr-my-4w")}>
        {header}
        <p>Chargement...</p>
      </div>
    );
  }

  if (!data || data.length === 0) {
    return (
      <div className={fr.cx("fr-my-4w")}>
        {header}
        <Alert
          severity="info"
          title="Aucune donnée disponible"
          description={
            period === "day"
              ? "Aucun trajet enregistré sur les jours récents pour cette campagne."
              : "Aucun trajet enregistré sur les derniers mois pour cette campagne."
          }
          small
        />
      </div>
    );
  }
  const name = ["Trajets avec Origine OU destination sur le territoire", "Trajets incités et validés par le RPC"];
  const colors = ["#6a6af4", "#000091"];
  const labels =
    period === "month"
      ? ([...new Set(data?.map((d) => `${String(d.month).padStart(2, "0")}/${d.year}`))] as string[])
      : ([...new Set(data?.map((d) => d.start_date))] as string[]);

  const datasets = [
    {
      data: labels.map((t) => {
        return period === "month"
          ? (data?.find((d) => `${String(d.month).padStart(2, "0")}/${d.year}` === t)?.journeys ?? 0)
          : (data?.find((d) => d.start_date === t)?.journeys ?? 0);
      }),
      fill: true,
      borderColor: colors[0],
      backgroundColor: `${colors[0]}33`,
      tension: 0.1,
      label: name[0],
    },
    {
      data: labels.map((t) => {
        return period === "month"
          ? (data?.find((d) => `${String(d.month).padStart(2, "0")}/${d.year}` === t)?.incented_journeys ?? 0)
          : (data?.find((d) => d.start_date === t)?.incented_journeys ?? 0);
      }),
      fill: true,
      borderColor: colors[1],
      backgroundColor: `${colors[1]}33`,
      tension: 0.1,
      label: name[1],
    },
  ];
  const chartData = { labels: labels, datasets: datasets };
  const options = {
    responsive: true,
    plugins: {
      legend: {
        display: true,
      },
    },
  };

  return (
    <div className={fr.cx("fr-my-4w")}>
      {header}
      <Line options={options} data={chartData} aria-hidden />
    </div>
  );
}
