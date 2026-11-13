import DownloadButton from '@/components/observatoire/DownloadButton';
import DeckMap from '@/components/observatoire/maps/DeckMap';
import { Config } from '@/config';
import { OBSERVATORY_API_URL } from '@/helpers/api';
import { classColor, getLegendClasses, jenks } from '@/helpers/analyse';
import { useApi } from '@/hooks/useApi';
import type { DensiteDataInterface } from '@/interfaces/observatoire/dataInterfaces';
import { fr } from '@codegouvfr/react-dsfr';
import type { PickingInfo } from '@deck.gl/core';
import { H3HexagonLayer } from '@deck.gl/geo-layers';
import bbox from '@turf/bbox';
import { multiPolygon } from '@turf/helpers';
import { cellsToMultiPolygon } from 'h3-js';
import { LngLatBoundsLike } from 'maplibre-gl';
import { useMemo } from 'react';
import { useDashboardContext } from '../../../../context/DashboardProvider';

// laisse lire les noms de lieux sous les mailles
const OPACITY = 0.8;

export default function DensiteMap({ title }: { title: string }) {
  const { dashboard } = useDashboardContext();
  const mapTitle = title;
  const url = () => {
    const params = [
      `code=${dashboard.params.code}`,
      `type=${dashboard.params.type}`,
      `year=${dashboard.params.year}`,
      `n=8`,
    ];
    switch (dashboard.params.period) {
      case "month":
        params.push(`month=${dashboard.params.month}`);
        break;
      case "trimester":
        params.push(`trimester=${dashboard.params.trimester}`);
        break;
      case "semester":
        params.push(`semester=${dashboard.params.semester}`);
        break;
    }
    return `${OBSERVATORY_API_URL}/location?${params.join("&")}`;
  };
  const { data, error, loading } = useApi<DensiteDataInterface[]>(url());
  const mapStyle = Config.get<string>('observatoire.mapStyle');

  const analyse = useMemo(() => {
    return data ? jenks(
      data,
        'count',
        // rampe validée à OPACITY sur le fond de carte : paliers distincts, classe claire visible
        ['#f8853a', '#e5600f', '#c44a06', '#8f3304', '#5a1c02'],
        [10, 10, 10, 10, 10],
      )
    : [];
  }, [data]);

  const layer = new H3HexagonLayer<DensiteDataInterface>({
    id: 'densite-layer',
    data: data ?? [],
    opacity: OPACITY,
    pickable: true,
    extruded: false,
    stroked: false,
    getHexagon: (d) => d.hex,
    getFillColor: (d) => classColor(d.count, analyse),
  });
  const bounds = () => {
    const hexagons = data?.map((d) => d.hex);
    const polygon = cellsToMultiPolygon(hexagons!, true);
    const bounds = dashboard.params.code === 'XXXXX' ? [-5.225, 41.333, 9.55, 51.2] : bbox(multiPolygon(polygon));
    return bounds as LngLatBoundsLike;
  };
  const tooltip = ({ object }: PickingInfo<DensiteDataInterface>) =>
    object ? {
      html: `<div><b>${object.count.toLocaleString('fr-FR')}</b> ${object.count > 1 ? 'départs ou arrivées' : 'départ ou arrivée'} de covoiturage dans cette maille</div>`,
      className: 'fr-callout',
      style: {
        color: '#000',
        backgroundColor: '#fff',
        fontSize: '0.8em',
        maxWidth: '250px',
      },
    } : null;

  return (
    <>
      {loading && (
        <div className={fr.cx('fr-callout')}>
          <h3 className={fr.cx('fr-callout__title')}>{title}</h3>
          <div>Chargement en cours...</div>
        </div>
      )}
      {error && (
        <div className={fr.cx('fr-callout')}>
          <h3 className={fr.cx('fr-callout__title')}>{title}</h3>
          <div>{`Un problème est survenu au chargement des données: ${error}`}</div>
        </div>
      )}
      {!data || data.length == 0 && (
        <div className={fr.cx('fr-callout')}>
          <h3 className={fr.cx('fr-callout__title')}>{title}</h3>
          <div>Pas de données disponibles pour cette carte...</div>
        </div>
      )}
      {!loading && !error && data && data.length > 0 && (
        <DeckMap 
          title={mapTitle} 
          tooltip={tooltip} 
          mapStyle={mapStyle} 
          layers={[layer]} 
          bounds={bounds()} 
          legend={[
            {
              title: 'Départs et arrivées par maille',
              subtitle: "Maille hexagonale d'environ 0,7 km²",
              type: 'interval',
              classes: getLegendClasses(analyse, 'interval'),
              note: 'Chaque trajet compte dans la maille de son départ et dans celle de son arrivée. Classes calculées sur la période affichée.',
              opacity: OPACITY,
            },
          ]}
          scrollZoom={false}
          download={
            <DownloadButton 
              title={'Télécharger les données de la carte'}
              data={data}
              filename='densite'
            />
          }
        />
      )}
    </>
  );
}
