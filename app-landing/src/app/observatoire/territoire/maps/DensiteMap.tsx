import DownloadButton from '@/components/observatoire/DownloadButton';
import DeckMap from '@/components/observatoire/maps/DeckMap';
import { Config } from '@/config';
import { classColor, jenks } from '@/helpers/analyse';
import { useApi } from '@/hooks/useApi';
import type { DensiteDataInterface } from '@/interfaces/observatoire/dataInterfaces';
import { fr } from '@codegouvfr/react-dsfr';
import { H3HexagonLayer } from '@deck.gl/geo-layers';
import bbox from '@turf/bbox';
import { multiPolygon } from '@turf/helpers';
import { cellsToMultiPolygon } from 'h3-js';
import { LngLatBoundsLike } from 'maplibre-gl';
import { useMemo } from 'react';
import { useDashboardContext } from '../../../../context/DashboardProvider';

export default function DensiteMap({ title }: { title: string }) {
  const { dashboard } = useDashboardContext();
  const mapTitle = title;
  const apiUrl = Config.get<string>('next.public_api_url', '');
  const url = `${apiUrl}/location?code=${dashboard.params.code}&type=${dashboard.params.type}&year=${dashboard.params.year}&month=${dashboard.params.month}&zoom=8`;
  const { data, error, loading } = useApi<DensiteDataInterface[]>(url);
  const mapStyle = Config.get<string>('observatoire.mapStyle');

  const analyse = useMemo(() => {
    return data ? jenks(
      data,
        'count',
        ['#fdd49e', '#fdbb84', '#fc8d59', '#e34a33', '#b30000', '#000000'],
        [10, 10, 10, 10, 10, 10],
      )
    : [];
  }, [data]);

  const layer = new H3HexagonLayer({
    id: 'densite-layer',
    data: data ? data : [],
    opacity: 0.4,
    pickable: true,
    extruded: false,
    lineWidthMinPixels: 1,
    getHexagon: (d) => d.hex,
    getFillColor: (d) => classColor(d.count, analyse),
    getLineColor: () => [80, 80, 80],
  });
  const bounds = () => {
    const hexagons = data?.map((d) => d.hex);
    const polygon = cellsToMultiPolygon(hexagons!, true);
    const bounds = dashboard.params.code === 'XXXXX' ? [-5.225, 41.333, 9.55, 51.2] : bbox(multiPolygon(polygon));
    return bounds as LngLatBoundsLike;
  };
  const tooltip = ({ object }: any) =>
    object && {
      html: `<div><b>${object.count.toLocaleString()}</b> départ(s) ou arrivée(s) de covoiturage dans cette maille hexagonale</div>`,
      className: 'fr-callout',
      style: {
        color: '#000',
        backgroundColor: '#fff',
        fontSize: '0.8em',
        width: '250px',
        height: '80px',
        left: '-125px',
        top: '-110px',
      },
    };

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
          scrollZoom={false}
          download={
            <DownloadButton 
              title={'Télécharger les données de la carte'}
              data={data!}
              filename='densite'
            />
          }
        />
      )}
    </>
  );
}
