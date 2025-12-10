import AppMap from '@/components/observatoire/maps/Map';
import { Config } from '@/config';
import { LineLayer, Layer, Source, Popup } from 'react-map-gl/maplibre';
import { LngLatBoundsLike } from 'maplibre-gl';
import { useJson } from '@/hooks/useJson';
import { Feature, FeatureCollection } from 'geojson';
import { useCallback, useMemo, useState } from 'react';
import Table from '@codegouvfr/react-dsfr/Table';
import SelectInList from '@/components/common/SelectInList';
import Link from 'next/link';
import bbox from '@turf/bbox';
import DownloadButton from '@/components/observatoire/DownloadButton';

export default function VrMap({ title}: { title: string }) {
  const mapTitle = title;
  const mapStyle = Config.get<string>('observatoire.mapStyle');
  const [bounds, setBounds] = useState<LngLatBoundsLike>([-5.225, 41.333, 9.55, 51.2]);
  const url = `https://static.covoiturage.beta.gouv.fr/voies_covoit_10ea0b9f65.json`;
  const { data } = useJson<FeatureCollection>(url);
  const geojson = useMemo(()=>{
    return data ? data : ''
  }, [data]);
  const selectList = [{id:0, name:'Sélectionner une voie'}]
  geojson ? geojson.features?.map((f,i) => {
    selectList.push({id:i+1, name:f.properties!.name})
  }): '';
  const [selected, setSelected] = useState(0);
  const [selectedData, setSelectedData] = useState<Feature>();
  const onChangeSelect = useCallback((value: number) => {
    setSelected(value);
    setSelectedData(geojson ? geojson.features[value-1] : undefined);
    setBounds((selectedData ? bbox(selectedData?.geometry) : [-5.225, 41.333, 9.55, 51.2]) as LngLatBoundsLike);
  },[geojson, selectedData]);
  const layer: LineLayer = {
    id: 'vr',
    source:'vr',
    type: 'line',
    paint: {
      'line-width': 10,  
      'line-color': '#000091',
      'line-opacity': 0.8
    },
  };
  const [cursor, setCursor] = useState<string>('');
  const [hoverInfo, setHoverInfo] = useState<{
    longitude: number,
    latitude: number,
    properties: any
  } | undefined>();
  const onMouseEnter = useCallback((e:any) => {
    setCursor('pointer');
    setHoverInfo({
      longitude: e.lngLat.lng,
      latitude: e.lngLat.lat,
      properties: e.features[0].properties
    });
  }, []);
  const onMouseLeave = useCallback(() => {
    setCursor('');
    setHoverInfo(undefined);
  }, []);

  return (
    <>
      <AppMap 
        title={mapTitle} 
        mapStyle={mapStyle} 
        bounds={bounds} 
        scrollZoom={false} 
        interactiveLayerIds={['vr']}
        sidebar={
          <>
            <SelectInList
              labelId='voies'
              label='Sélectionner une voie'
              id={selected}
              list={selectList}
              sx={{ minWidth: 300 }}
              onChange={onChangeSelect}
            />
            {
              selectedData && selectedData.properties && <Table data={[
                ['Type', selectedData.properties.type],
                ['Gestion', selectedData.properties.gestionnaire],
                ['Localisation', selectedData.properties.localisation],
                ['Longueur', selectedData.properties.distance],
                ['Année de mise en service', selectedData.properties.mise_en_service],
                [<Link key={selectedData.properties.name} href={selectedData.properties.link} target='_blank'>En savoir +</Link>],
              ]} colorVariant={'blue-ecume'} />
            }
          </>          
        }
        sidebarPosition='right'
        sidebarWidth={4}
        cursor={cursor}
        onMouseEnter={onMouseEnter}
        onMouseLeave={onMouseLeave}
        download={
          <DownloadButton 
            title={'Télécharger les données de la carte'}
            data={geojson as FeatureCollection}
            type={'geojson'}
            filename='voies_reservees_covoiturage'
          />
        }
      >
        <Source id='vr' type='geojson' data={geojson}>
          <Layer {...layer} />
          <Popup longitude={hoverInfo ? hoverInfo.longitude : 0} latitude={hoverInfo ? hoverInfo.latitude : 0} closeButton={false}>
              {hoverInfo && 
                <div>
                  {hoverInfo.properties.name && <p><b>nom : </b>{hoverInfo.properties.name}</p>}
                </div>
              }
            </Popup>
        </Source>
      </AppMap>
    </>
  );

}