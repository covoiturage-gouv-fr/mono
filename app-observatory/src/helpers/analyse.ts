import { ClasseInterface } from '@/interfaces/observatoire/componentsInterfaces';
import type { AnalyseInterface } from '@/interfaces/observatoire/helpersInterfaces';
import { ckmeans } from 'simple-statistics';

export function jenks<K extends string>(data: Record<K, number>[], field: K, colors: string[], width: number[]) {
  const vals: number[] = data.map((d) => d[field]);
  const nbClasses = Math.min(colors.length, vals.length);
  const breaks =  nbClasses > 1 ? ckmeans(vals, nbClasses) : [];
  const analysis: AnalyseInterface[] = breaks.map((b, i) => {
    return { val: b[0], max: b[b.length - 1], color: hexToRgb(colors[i]), width: width[i] };
  });
  return analysis;
}

export function hexToRgb(hex: string): [number, number, number] {
  const m = /^#?([\da-f]{2})([\da-f]{2})([\da-f]{2})$/i.exec(hex);
  if (m) {
    const rgb: [number, number, number] = [parseInt(m[1], 16), parseInt(m[2], 16), parseInt(m[3], 16)];
    return rgb;
  } else {
    return [0, 0, 0];
  }
}

// Classe qui contient la valeur : la dernière dont le seuil bas est <= val (classes triées).
function classOf(val: number, analysis: AnalyseInterface[]) {
  return analysis.findLast((d) => d.val <= val) ?? analysis[0];
}

export function classColor(val: number, analysis: AnalyseInterface[]) {
  return classOf(val, analysis)?.color || [0, 0, 0];
}

export function classWidth(val: number, analysis: AnalyseInterface[]) {
  return classOf(val, analysis)?.width || 1;
}

export function getPeriod(year: number, month: number) {
  const period = {
    start_date: new Date(year, month - 1, 2).toISOString().slice(0, 10),
    end_date: new Date(year, month).toISOString().slice(0, 10),
  };
  return period;
}

export function getLegendClasses(analysis: AnalyseInterface[], type: string){
  const legend:ClasseInterface[] =  []
  if (analysis.length > 1){
    if(type === 'interval' && analysis.every((d) => d.max !== undefined)){
      const fmt = (n: number) => n.toLocaleString('fr-FR');
      for(const d of analysis){
        legend.push({
          color: d.color,
          val: d.val === d.max ? fmt(d.val) : `${fmt(d.val)} – ${fmt(d.max!)}`,
          width: d.width,
        })
      }
    } else if(type === 'interval'){
      for(let i = 0; i < analysis.length; i++){
        const d = analysis[i]
        const classe = {
          color:[255,255,255],
          val:'',
          width:0
        }
        if(i === analysis.length-1){
          classe.color = d.color
          classe.val = `>= ${d.val}`
          classe.width =  d.width
          
        } else {
          classe.color = d.color
          classe.val = `${d.val} à ${(analysis[i+1].val)}`
          classe.width =  d.width
        }
        legend.push(classe)
      }
    } else if(['category','proportional_circles'].includes(type)){
      for(const d of analysis){
        const classe = {
          color: d.color,
          val : `${d.val}`,
          width :  d.width
        }
        legend.push(classe)
      }
    }
  }
  return legend
}
