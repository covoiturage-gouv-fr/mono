import bbox from '@turf/bbox';
import { FeatureCollection } from 'geojson';
import type { LngLatBoundsLike, MapGeoJSONFeature } from 'maplibre-gl';
import { MapRef } from 'react-map-gl/maplibre';

export function getBbox(data: MapGeoJSONFeature) {
  const bounds = bbox(data);
  const coords: LngLatBoundsLike = [bounds[0], bounds[1], bounds[2], bounds[3]];
  return coords;
}

export const fitBounds = (map: MapRef | null, bounds?: LngLatBoundsLike) => {
  if (map && bounds && Array.isArray(bounds) && !bounds.flat().includes(Infinity)) {
    map.fitBounds(bounds, { padding: 20 });
  }
};

export const parseJSONFile = async <T = unknown>(path:string) => {
  const response = await fetch(path);
  const text = await response.text();
  const parseData = JSON.parse(text) as T;
  return parseData;
}

export const downloadData = <T extends object>(filename:string, data:T[] | FeatureCollection, type?:'csv' | 'geojson') => {
  let blob = new Blob([JSON.stringify(data)],{type:'application/json'});
  if (type === 'csv' && Array.isArray(data)){
    const header = Object.keys(data[0]) as (keyof T)[];
    const replacer = (value:unknown) => value === null ? '' : value;
    const csv = [
      header.join(','), // header row first
      ...data.map((row) => header.map((fieldName) => JSON.stringify(replacer(row[fieldName]))).join(','))
    ].join('\r\n');
    blob = new Blob([csv],{type:'text/csv'});
  } 
  const fileURL = window.URL.createObjectURL(blob);
  const fileLink = document.createElement('a');
  fileLink.href = fileURL;
  fileLink.title = `Télécharger le ficher ${filename}.${type ?? 'json'}`;
  fileLink.setAttribute('download', `${filename}.${type ?? 'json'}`);
  document.body.appendChild(fileLink);
  fileLink.click();
}
