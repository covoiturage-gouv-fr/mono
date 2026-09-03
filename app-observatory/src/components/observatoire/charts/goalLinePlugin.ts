import { Plugin } from "chart.js";

const RED = "#e1000f";

// Trace une ligne horizontale d'objectif sur un graphe à barres (évite d'ajouter
// la dépendance chartjs-plugin-annotation). Configuré via le plugin option `goalLine`
// (`{ value, label }`) posé dans `options.plugins.goalLine`.
export const goalLinePlugin: Plugin<"bar"> = {
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
