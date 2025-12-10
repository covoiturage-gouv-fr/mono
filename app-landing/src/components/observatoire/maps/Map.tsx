'use client';
import { fitBounds } from '@/helpers/map';
import { MapInterface, ViewInterface } from '@/interfaces/observatoire/componentsInterfaces';
import { FrCxArg, fr } from '@codegouvfr/react-dsfr';
import 'maplibre-gl/dist/maplibre-gl.css';
import { useCallback, useEffect, useRef, useState } from 'react';
import Map, { FullscreenControl, MapRef, NavigationControl } from 'react-map-gl/maplibre';
import Legend from './Legend';

const AppMap = (props: MapInterface) => {
  const mapRef = useRef<MapRef>(null);
  const defaultView: ViewInterface = {
    latitude: 46.9,
    longitude: 1.7,
    zoom: 5,
  };
  
  useEffect(() => {
    fitBounds(mapRef.current, props.bounds);
  }, [props.bounds]);
  
  const [cursor, setCursor] = useState<string>('');
  const defaultOnMouseEnter = useCallback(() => setCursor('pointer'), []);
  const defaultOnMouseLeave = useCallback(() => setCursor(''), []);

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
          <div className={fr.cx('fr-col-12', sidebarClass)}>
            {props.sidebar}
          </div>
        }
        <div className={fr.cx('fr-col-12', mapClass)} tabIndex={0}>
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
            cursor={props.cursor ? props.cursor : cursor}
            onMouseEnter={props.onMouseEnter ? props.onMouseEnter : defaultOnMouseEnter}
            onMouseLeave={props.onMouseLeave ? props.onMouseLeave : defaultOnMouseLeave}
            onMouseMove={props.onMouseMove}
            onClick={props.onClick}
            interactiveLayerIds={props.interactiveLayerIds}
          >
            <NavigationControl />
            <FullscreenControl />
            {props.legend &&
              props.legend.map((l,i) =>
                <Legend key={i} title={l.title} type={l.type} classes={l.classes} order={l.order} />
              )
            }
            {props.children}
          </Map>
        </div>
        {props.sidebar && props.sidebarPosition == 'right' &&
          <div className={fr.cx('fr-col-12', sidebarClass)}>
            {props.sidebar}
          </div>
        }
      </div>
    </div>
  );
};

export default AppMap;
