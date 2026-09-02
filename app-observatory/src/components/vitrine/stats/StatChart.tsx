"use client";

import { fr } from "@codegouvfr/react-dsfr";
import {
  BarElement,
  CategoryScale,
  ChartData,
  Chart as ChartJS,
  Filler,
  Legend,
  LinearScale,
  LineElement,
  Plugin,
  PointElement,
  Title,
  Tooltip,
} from "chart.js";
import ChartDataLabels from "chartjs-plugin-datalabels";
import { Bar, Line } from "react-chartjs-2";

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  PointElement,
  LineElement,
  Filler,
  Title,
  Tooltip,
  Legend,
);

const BLUE = "#000091";
const RED = "#e1000f";

// Trace une ligne horizontale d'objectif sur un graphe à barres (évite d'ajouter
// la dépendance chartjs-plugin-annotation).
const goalLinePlugin: Plugin<"bar"> = {
  id: "goalLine",
  afterDatasetsDraw(chart, _args, options) {
    const opts = options as { value?: number; label?: string };
    if (!opts?.value) return;
    const y = chart.scales.y;
    if (!y || opts.value > y.max) return;
    const {
      ctx,
      chartArea: { left, right },
    } = chart;
    const yPos = y.getPixelForValue(opts.value);
    ctx.save();
    ctx.beginPath();
    ctx.setLineDash([6, 6]);
    ctx.lineWidth = 1.5;
    ctx.strokeStyle = RED;
    ctx.moveTo(left, yPos);
    ctx.lineTo(right, yPos);
    ctx.stroke();
    if (opts.label) {
      ctx.setLineDash([]);
      ctx.fillStyle = RED;
      ctx.font = "12px Marianne, sans-serif";
      ctx.textAlign = "right";
      ctx.textBaseline = "bottom";
      ctx.fillText(opts.label, right, yPos - 4);
    }
    ctx.restore();
  },
};

type StatChartProps = {
  title: string;
  labels: string[];
  values: number[];
  kind?: "bar" | "line";
  unit?: string;
  /** Affiche la valeur au-dessus de chaque barre (séries courtes uniquement). */
  showValues?: boolean;
  /** Ligne horizontale d'objectif (barres uniquement). */
  goal?: number;
  goalLabel?: string;
  loading?: boolean;
  error?: string;
};

const format = (value: number, unit?: string): string => {
  const n = value.toLocaleString("fr-FR", { maximumFractionDigits: 2 });
  return unit ? `${n} ${unit}` : n;
};

export default function StatChart({
  title,
  labels,
  values,
  kind = "bar",
  unit,
  showValues = false,
  goal,
  goalLabel,
  loading = false,
  error,
}: StatChartProps) {
  if (loading || error || values.length === 0) {
    if (error) console.error(`StatChart "${title}" :`, error);
    return (
      <div className={fr.cx("fr-callout")}>
        <h3 className={fr.cx("fr-callout__title", "fr-text--xl")}>{title}</h3>
        <div>
          {loading
            ? "Chargement en cours…"
            : error
              ? "Un problème est survenu au chargement des données."
              : "Pas de données disponibles pour ce graphique."}
        </div>
      </div>
    );
  }

  const options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { display: false },
      goalLine: goal ? { value: goal, label: goalLabel } : { value: undefined },
    },
    scales: goal ? { y: { suggestedMax: goal * 1.05 } } : undefined,
  };
  const plugins: any = [];
  if (showValues) plugins.push(ChartDataLabels);
  if (goal) plugins.push(goalLinePlugin);

  const datasets = [
    {
      label: title,
      data: values,
      borderColor: BLUE,
      backgroundColor: kind === "line" ? "rgba(0, 0, 145, 0.2)" : BLUE,
      fill: kind === "line",
      tension: 0.1,
      datalabels: showValues
        ? {
            anchor: "end",
            align: "end",
            font: { size: 10 },
            formatter: (value: number) => format(value, unit),
          }
        : { display: false },
    },
  ];
  const data = { labels, datasets };

  return (
    <div className={fr.cx("fr-callout")}>
      <div className={fr.cx("fr-callout__title", "fr-text--xl")}>{title}</div>
      <figure
        className="graph-wrapper"
        style={{ backgroundColor: "#fff", height: "320px" }}
      >
        {kind === "line" ? (
          <Line
            options={options}
            data={data as ChartData<"line", number[]>}
            aria-hidden
          />
        ) : (
          <Bar
            options={options}
            plugins={plugins}
            data={data as ChartData<"bar", number[]>}
            aria-hidden
          />
        )}
        <figcaption className={fr.cx("fr-sr-only")}>
          {goal && (
            <p>
              {goalLabel ?? "Objectif"} : {format(goal, unit)}
            </p>
          )}
          <ul>
            {values.map((value, i) => (
              <li key={i}>
                {labels[i]} : {format(value, unit)}
              </li>
            ))}
          </ul>
        </figcaption>
      </figure>
    </div>
  );
}
