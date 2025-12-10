import DownloadButton from '@/components/observatoire/DownloadButton';
import DeckMap from '@/components/observatoire/maps/DeckMap';
import { Config } from '@/config';
import { classWidth, getLegendClasses, jenks } from '@/helpers/analyse';
import { GetApiUrl } from '@/helpers/api';
import { useApi } from '@/hooks/useApi';
import type { FluxDataInterface } from '@/interfaces/observatoire/dataInterfaces';
import { fr } from '@codegouvfr/react-dsfr';
import { ArcLayer } from '@deck.gl/layers';
import bbox from '@turf/bbox';
import { multiPoint } from '@turf/helpers';
import { LngLatBoundsLike } from 'maplibre-gl';
import { useMemo } from 'react';
import { useDashboardContext } from '../../../../context/DashboardProvider';

export default function FluxMap({ title }: { title: string }) {
  const { dashboard } = useDashboardContext();
  const params = [
    `code=${dashboard.params.code}`,
    `type=${dashboard.params.type}`,
    `observe=${dashboard.params.observe}`,
    `year=${dashboard.params.year}`
  ];
  const url = GetApiUrl('flux', params);
  const { data, error, loading } = useApi<FluxDataInterface[]>(url);
  const mapStyle = Config.get<string>('observatoire.mapStyle');
  const filteredData = useMemo(() => {
    return data ? data.filter(d=>d.passengers >= 10) : []
  }, [data]);

  const analyse = useMemo(() => {
    return filteredData ? jenks(
      filteredData,
        'passengers',
        ['#000091', '#000091', '#000091', '#000091', '#000091', '#000091'],
        [1, 3, 6, 12, 24, 48],
      )
    : [];
  }, [filteredData]);

  const layer = new ArcLayer({
    id: 'flux-layer',
    data: filteredData,
    opacity: 0.3,
    pickable: true,
    getWidth: (d) => classWidth(d.passengers, analyse),
    getSourcePosition: (d) => [d.lng_1, d.lat_1],
    getTargetPosition: (d) => [d.lng_2, d.lat_2],
    getSourceColor: [0, 0, 145],
    getTargetColor: [0, 0, 145],
  });

  const fitBounds = () => {
    const coords = filteredData.map((d) => {
      return [
        [d.lng_1, d.lat_1],
        [d.lng_2, d.lat_2],
      ];
    })
    .reduce((acc, val) => acc.concat(val), []);
    const bounds = dashboard.params.code === 'XXXXX' ? [-5.225, 41.333, 9.55, 51.2] : bbox(multiPoint(coords));
    return  bounds as LngLatBoundsLike;
  };

  const tooltip = ({ object }: any) =>
    object && {
      html: `<div class="tooltip-title"><b>${object.ter_1} - ${object.ter_2}</b></div>
    <div>${object.passengers} passagers transportés</div>
    <div>${object.distance.toLocaleString()} Km parcourus</div>`,
      className: 'fr-callout',
      style: {
        color: '#000',
        backgroundColor: '#fff',
        fontSize: '1em',
        width: '250px',
        height: '110px',
        left: '-125px',
        top: '-110px',
      },
    };

  return (
    <>
      {loading && (
        <div className={fr.cx('fr-callout')}>
          <h3 className={fr.cx('fr-callout__title')}>
            {title}
          </h3>
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
          title={title} 
          tooltip={tooltip} 
          mapStyle={mapStyle} 
          layers={[layer]} 
          bounds={fitBounds()}
          scrollZoom={false}
          legend={[
            {
              title: title,
              type:'interval',
              classes: getLegendClasses(analyse,'interval')
            }
          ]}
          download={
            <DownloadButton 
              title={'Télécharger les données de la carte'}
              data={filteredData}
              filename='flux'
            />
          }
        />
      )}
    </>
  );
}
