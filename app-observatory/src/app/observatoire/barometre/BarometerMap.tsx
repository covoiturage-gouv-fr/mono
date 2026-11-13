'use client';
import AppMap from '@/components/observatoire/maps/Map';
import { Config } from '@/config';
import { pointsToGeoJSON } from '@/helpers/barometer';
import type { BarometerPoint, BarometerProvider } from '@/interfaces/observatoire/barometer';
import { fr } from '@codegouvfr/react-dsfr';
import bbox from '@turf/bbox';
import type { ExpressionSpecification, LngLatBoundsLike, MapLayerMouseEvent } from 'maplibre-gl';
import { ReactNode, useCallback, useMemo, useState } from 'react';
import { CircleLayerSpecification, Layer, Popup, Source } from 'react-map-gl/maplibre';

const FRANCE_BOUNDS: LngLatBoundsLike = [-5.225, 41.333, 9.55, 51.2];

const hexToRgb = (hex: string) => [1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16));

export default function BarometerMap({
  points,
  providers,
  colors,
  selectedId,
  loading,
  error,
  sidebar,
  onSelect,
}: {
  points: BarometerPoint[];
  providers: BarometerProvider[];
  colors: Record<string, string>;
  selectedId: number | null;
  loading: boolean;
  error: unknown;
  sidebar: ReactNode;
  onSelect: (id: number) => void;
}) {
  const geojson = useMemo(() => pointsToGeoJSON(points), [points]);
  // Un point seul donnerait une emprise nulle : on reste sur la France.
  // Marge d'un degré pour que les points au bord ne passent pas sous la légende ou l'attribution.
  const bounds = useMemo<LngLatBoundsLike>(() => {
    if (points.length < 2) return FRANCE_BOUNDS;
    const [minX, minY, maxX, maxY] = bbox(geojson);
    return [minX - 1, minY - 1, maxX + 1, maxY + 1];
  }, [geojson, points.length]);

  const paint = useMemo<CircleLayerSpecification['paint']>(() => {
    const isSelected: ExpressionSpecification = ['==', ['get', 'id'], selectedId ?? -1];
    // `match` exige au moins un couple valeur/couleur : sans fournisseur, couleur fixe.
    const [first, ...others] = providers;
    const color: ExpressionSpecification | string = first
      ? ['match', ['get', 'fournisseur_id'], first.id, colors[first.id],
        ...others.flatMap((p) => [p.id, colors[p.id]]), '#666666']
      : '#666666';
    return {
      'circle-color': color,
      'circle-radius': ['case', isSelected, 10, 7],
      'circle-stroke-color': '#ffffff',
      'circle-stroke-width': ['case', isSelected, 3, 2],
    };
  }, [providers, colors, selectedId]);

  const [hover, setHover] = useState<BarometerPoint>();
  const [cursor, setCursor] = useState('');
  const onMouseEnter = useCallback((e: MapLayerMouseEvent) => {
    setCursor('pointer');
    setHover(e.features?.[0]?.properties as BarometerPoint | undefined);
  }, []);
  const onMouseLeave = useCallback(() => {
    setCursor('');
    setHover(undefined);
  }, []);
  const onClick = useCallback(
    (e: MapLayerMouseEvent) => {
      const point = e.features?.[0]?.properties as BarometerPoint | undefined;
      if (point) onSelect(point.id);
    },
    [onSelect],
  );

  if (loading || error) {
    return (
      <div className={fr.cx('fr-callout')}>
        {loading ? 'Chargement en cours…' : 'Un problème est survenu au chargement des points de captage.'}
      </div>
    );
  }

  return (
    <AppMap
      mapStyle={Config.get<string>('observatoire.mapStyle')}
      bounds={bounds}
      scrollZoom={false}
      height='450px'
      interactiveLayerIds={['barometer-points']}
      cursor={cursor}
      onMouseEnter={onMouseEnter}
      onMouseLeave={onMouseLeave}
      onClick={onClick}
      sidebar={sidebar}
      sidebarPosition='left'
      sidebarWidth={4}
      legend={[
        {
          title: 'Fournisseurs de données',
          type: 'category',
          classes: providers.map((p) => ({ color: hexToRgb(colors[p.id]), val: p.name, width: 12 })),
        },
      ]}
    >
      <Source id='barometer' type='geojson' data={geojson}>
        <Layer id='barometer-points' type='circle' paint={paint} />
      </Source>
      {hover && (
        <Popup longitude={hover.lon} latitude={hover.lat} closeButton={false} offset={10}>
          <b>{hover.nom}</b>
          <br />
          {hover.fournisseur_nom}
        </Popup>
      )}
    </AppMap>
  );
}
