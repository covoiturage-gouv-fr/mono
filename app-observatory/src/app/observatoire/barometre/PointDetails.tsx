'use client';
import Chart from '@/components/observatoire/charts/Chart';
import { barometerMeasuresUrl, chartsOf, periodLabel } from '@/helpers/barometer';
import { useApi } from '@/hooks/useApi';
import type { BarometerMeasure, BarometerPoint } from '@/interfaces/observatoire/barometer';
import { fr } from '@codegouvfr/react-dsfr';
import { ReactNode, useMemo } from 'react';

function Tile({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div className={fr.cx('fr-col-12', 'fr-col-md-4')}>
      <div className={fr.cx('fr-p-3w')} style={{ height: '100%', border: '1px solid var(--border-default-grey)' }}>
        <h3 className={fr.cx('fr-text--sm', 'fr-text--bold', 'fr-mb-2w')} style={{ textTransform: 'uppercase' }}>
          {title}
        </h3>
        {children}
      </div>
    </div>
  );
}

function Lines({ lines }: { lines: string[] }) {
  return lines.map((line, i) => (
    <p key={i} className={fr.cx('fr-text--sm', i === lines.length - 1 && 'fr-mb-0')}>
      {line}
    </p>
  ));
}

function characteristics(p: BarometerPoint): string[] {
  const lanes = [
    p.voie_droite && 'droite',
    p.voie_milieu && 'milieu',
    p.voie_gauche && 'gauche',
  ].filter(Boolean);
  return [
    [p.axe, p.sens && `sens ${p.sens}`, p.pk !== null && `PK ${p.pk.toLocaleString('fr-FR')}`]
      .filter(Boolean)
      .join(', '),
    p.departement && `Département ${p.departement}`,
    lanes.length > 0 && `Voies observées : ${lanes.join(', ')}`,
  ].filter((l): l is string => !!l);
}

function methodology(p: BarometerPoint, measures: BarometerMeasure[]): string[] {
  const periods = Array.from(
    new Map([...measures].sort((a, b) => a.debut.localeCompare(b.debut))
      .map((m) => [`${m.debut}/${m.fin}`, periodLabel(m.debut, m.fin)])).values(),
  );
  // Seules les lignes horaires sont des comptages : les autres sont des moyennes, non additionnables.
  const counted = measures.filter((m) => m.heure !== null).reduce((sum, m) => sum + m.nb_vl, 0);
  return [
    p.technologie && `Capteur : ${p.technologie}${p.infrarouge ? ', infrarouge' : ''}${p.comptage_arriere ? ', comptage arrière' : ''}`,
    periods.length > 0 && `Période${periods.length > 1 ? 's' : ''} de mesure : ${periods.join(', ')}`,
    counted > 0 && `Véhicules légers comptés : ${counted.toLocaleString('fr-FR')}`,
  ].filter((l): l is string => !!l);
}

export default function PointDetails({ point }: { point: BarometerPoint }) {
  const url = useMemo(() => barometerMeasuresUrl(point.id), [point.id]);
  const { data, error, loading } = useApi<BarometerMeasure[]>(url);
  const measures = useMemo(() => data ?? [], [data]);
  const charts = useMemo(() => chartsOf(measures), [measures]);

  return (
    <section className={fr.cx('fr-mt-5w')}>
      <h2 className={fr.cx('fr-h4')}>Résultats pour le point de captage {point.nom}</h2>
      {loading || error ? (
        <div className={fr.cx('fr-callout')}>
          {loading ? 'Chargement en cours…' : 'Un problème est survenu au chargement des mesures.'}
        </div>
      ) : (
        <>
          <div className={fr.cx('fr-grid-row', 'fr-grid-row--gutters')}>
            <Tile title='Méthodologie'>
              <Lines lines={methodology(point, measures)} />
            </Tile>
            <Tile title='Organisme responsable'>
              <Lines lines={[point.fournisseur_nom, point.gestionnaire && `Gestionnaire : ${point.gestionnaire}`]
                .filter((l): l is string => !!l)} />
            </Tile>
            <Tile title='Caractéristiques du point'>
              <Lines lines={characteristics(point)} />
            </Tile>
          </div>
          <h3 className={fr.cx('fr-h5', 'fr-mt-5w')}>Analyses et tendances</h3>
          {charts.length === 0 ? (
            <p>Pas de mesure publiée pour ce point.</p>
          ) : (
            <div className={fr.cx('fr-grid-row', 'fr-grid-row--gutters')}>
              {charts.map((c) => (
                <div key={c.key} className={fr.cx('fr-col-12')}>
                  <Chart
                    title={c.title}
                    kind={c.kind}
                    labels={c.labels}
                    data={c.series}
                    unit='%'
                    height={280}
                    legend={c.series.length > 1}
                    download={{ data: measures, filename: `barometre-${point.id}` }}
                  />
                </div>
              ))}
            </div>
          )}
        </>
      )}
    </section>
  );
}
