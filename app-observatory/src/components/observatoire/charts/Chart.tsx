"use client";

import DownloadButton from "@/components/observatoire/DownloadButton";
import { fr } from "@codegouvfr/react-dsfr";
import {
  ArcElement,
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
import { Bar, Doughnut, Line } from "react-chartjs-2";
import { goalLinePlugin } from "./goalLinePlugin";

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  PointElement,
  LineElement,
  ArcElement,
  Filler,
  Title,
  Tooltip,
  Legend,
);

// Palette DSFR : bleu France en tête, puis teintes de contraste. Sert aussi bien
// pour colorer plusieurs séries que pour les parts d'un doughnut.
const PALETTE = ["#000091", "#6a6af4", "#b7a73f", "#e1000f", "#3182bd", "#9ecae1"];

export type ChartSeries = {
  label: string;
  data: number[];
  /** Couleur de la série (ligne / barre). */
  color?: string;
  /** Couleurs par point — doughnut uniquement (défaut : palette). */
  colors?: string[];
  fill?: boolean;
  /** Config `chartjs-plugin-datalabels` propre à la série (opt-in). */
  datalabels?: unknown;
};

export type ChartProps = {
  title: string;
  kind?: "line" | "bar" | "doughnut";
  labels: string[];
  /** `number[]` = série unique sans libellé. */
  data: number[] | ChartSeries[];
  unit?: string;
  formatValue?: (value: number) => string;
  height?: number;
  legend?: boolean;
  /** Ligne horizontale d'objectif (barres uniquement). */
  goal?: number;
  goalLabel?: string;
  /** Affiche la valeur au-dessus de chaque barre (séries courtes uniquement). */
  showValues?: boolean;
  /** Affiche un bouton de téléchargement CSV des données passées. */
  download?: { data: unknown[]; filename: string };
  /** Paragraphe(s) d'introduction de la figcaption lecteur d'écran (1 par série). */
  srIntro?: string | string[];
  loading?: boolean;
  error?: unknown;
};

const toSeries = (data: number[] | ChartSeries[]): ChartSeries[] =>
  Array.isArray(data) && typeof data[0] === "number"
    ? [{ label: "", data: data as number[] }]
    : (data as ChartSeries[]);

export default function Chart({
  title,
  kind = "bar",
  labels,
  data,
  unit,
  formatValue,
  height = 320,
  legend,
  goal,
  goalLabel,
  showValues = false,
  download,
  srIntro,
  loading = false,
  error,
}: ChartProps) {
  const series = toSeries(data);
  const format =
    formatValue ??
    ((value: number) => {
      const n = value.toLocaleString("fr-FR", { maximumFractionDigits: 2 });
      return unit ? `${n} ${unit}` : n;
    });

  if (loading || error || series.every((s) => s.data.length === 0)) {
    if (error) console.error(`Chart "${title}" :`, error);
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

  const showLegend = legend ?? series.length > 1;
  const options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { display: showLegend },
      goalLine: goal ? { value: goal, label: goalLabel } : { value: undefined },
    },
    scales: goal ? { y: { suggestedMax: goal * 1.05 } } : undefined,
  };

  const plugins: Plugin[] = [];
  if (showValues || series.some((s) => s.datalabels)) plugins.push(ChartDataLabels);
  if (goal) plugins.push(goalLinePlugin as Plugin);

  const datasets = series.map((s, i) => {
    const color = s.color ?? PALETTE[i % PALETTE.length];
    const fill = s.fill ?? kind === "line";
    return {
      label: s.label,
      data: s.data,
      borderColor: color,
      backgroundColor:
        kind === "doughnut"
          ? (s.colors ?? s.data.map((_, j) => PALETTE[j % PALETTE.length]))
          : kind === "line" && fill
            ? `${color}33`
            : color,
      fill,
      tension: 0.1,
      datalabels:
        s.datalabels ??
        (showValues
          ? {
              anchor: "end",
              align: "end",
              font: { size: 10 },
              formatter: (value: number) => format(value),
            }
          : { display: false }),
    };
  });

  const intros = srIntro
    ? Array.isArray(srIntro)
      ? srIntro
      : [srIntro]
    : [];

  const chartData = { labels, datasets };
  const common = { options, plugins, "aria-hidden": true } as const;

  return (
    <div className={fr.cx("fr-callout")}>
      <div className={fr.cx("fr-callout__title", "fr-text--xl")}>
        {title}
        {download && (
          <span className={fr.cx("fr-pl-5v")}>
            <DownloadButton
              title="Télécharger les données du graphique"
              data={download.data as never}
              filename={download.filename}
            />
          </span>
        )}
      </div>
      <figure
        className="graph-wrapper"
        style={{ backgroundColor: "#fff", height: `${height}px` }}
      >
        {kind === "line" ? (
          <Line {...common} data={chartData as ChartData<"line", number[]>} />
        ) : kind === "doughnut" ? (
          <Doughnut
            {...common}
            data={chartData as ChartData<"doughnut", number[]>}
          />
        ) : (
          <Bar {...common} data={chartData as ChartData<"bar", number[]>} />
        )}
        <figcaption className={fr.cx("fr-sr-only")}>
          {goal && (
            <p>
              {goalLabel ?? "Objectif"} : {format(goal)}
            </p>
          )}
          {series.map((s, i) => (
            <div key={i}>
              {intros[i] && <p>{intros[i]}</p>}
              <ul>
                {s.data.map((value, j) => (
                  <li key={j}>
                    {labels[j]} : {format(value)}
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </figcaption>
      </figure>
    </div>
  );
}
