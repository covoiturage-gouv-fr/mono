'use client';
import { fitBounds } from '@/helpers/map';
import { DeckMapInterface } from '@/interfaces/observatoire/componentsInterfaces';
import { FrCxArg, fr } from '@codegouvfr/react-dsfr';
import { MapboxOverlay, MapboxOverlayProps } from '@deck.gl/mapbox';
import 'maplibre-gl/dist/maplibre-gl.css';
import { useEffect, useRef } from 'react';
import { FullscreenControl, Map, MapRef, NavigationControl, useControl } from 'react-map-gl/maplibre';
import Legend from './Legend';

function DeckGLOverlay(
  props: MapboxOverlayProps & {
    interleaved?: boolean;
  },
) {
  const overlay = useControl<MapboxOverlay>(() => new MapboxOverlay(props));
  overlay.setProps(props);
  return null;
}

const DeckMap = (props: DeckMapInterface) => {
  const mapRef = useRef<MapRef>(null);
  const defaultView = {
    latitude: 46.9,
    longitude: 1.7,
    zoom: 5,
  };
 
  useEffect(() => {
    fitBounds(mapRef.current, props.bounds);
  }, [props.bounds]);

  const inRange = (value: number, min: number, max: number) => {
    return Math.trunc(value) >=min && Math.trunc(value) <= max ? true : false;
  }
  const sidebarClass = props.sidebarWidth && inRange(props.sidebarWidth,1,12) ? `fr-col-md-${Math.trunc(props.sidebarWidth)}` as FrCxArg : undefined;
  const mapClass = props.sidebarWidth && inRange(props.sidebarWidth,1,12) ? `fr-col-md-${12 - Math.trunc(props.sidebarWidth)}` as FrCxArg : undefined;

  return (
    <div className='fr-callout'>
      <h3 className='fr-callout__title'>
        {props.title}
        { props.download &&
          <span className={fr.cx('fr-ml-5v')}>{props.download}</span>
        } 
      </h3>
      <div className={fr.cx('fr-grid-row','fr-grid-row--gutters')}>
        {props.sidebar && props.sidebarPosition == 'left' &&
          <div className={fr.cx('fr-col', sidebarClass)}>
            {props.sidebar}
          </div>
        }
        <div className={fr.cx('fr-col', mapClass)} tabIndex={0}>
          <Map
            ref={mapRef}
            initialViewState={props.initialView ? props.initialView : defaultView}
            style={{
              width: props.width ? props.width : '100%',
              height: props.height ? props.height : '60vh',
            }}
            mapStyle={props.mapStyle}
            onLoad={() => fitBounds(mapRef.current, props.bounds)}
            scrollZoom={props.scrollZoom}
          >
            <DeckGLOverlay layers={props.layers} getTooltip={props.tooltip} />
            <NavigationControl />
            <FullscreenControl />
            {props.legend &&
              props.legend.map((l,i) =>
                <Legend key={i} title={l.title} type={l.type} classes={l.classes} order={l.order} />
              )
            }
          </Map>
        </div>
        {props.sidebar && props.sidebarPosition == 'right' &&
          <div className={fr.cx('fr-col', sidebarClass)}>
            {props.sidebar}
          </div>
        }
      </div>
    </div>
  );
};

export default DeckMap;
