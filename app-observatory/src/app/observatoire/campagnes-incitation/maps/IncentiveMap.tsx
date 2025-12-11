import AppMap from "@/components/observatoire/maps/Map";
import { Config } from "@/config";
import { fr } from "@codegouvfr/react-dsfr";
import bbox from "@turf/bbox";
import { FeatureCollection } from "geojson";
import { LngLatBoundsLike, MapLayerMouseEvent } from "maplibre-gl";
import { ReactNode, useCallback, useMemo, useState } from "react";
import { FillLayer, Layer, Popup, Source } from "react-map-gl/maplibre";

import DownloadButton from "@/components/observatoire/DownloadButton";
import { getUrl } from "@/helpers/search";
import { INSEECode, PerimeterType } from "@/interfaces/observatoire/Perimeter";
import { useRouter } from "next/navigation";

export default function IncentiveMap({
  params,
  data,
  loading,
  error,
  sidebar,
}: {
  params: { code: INSEECode; type: PerimeterType; year: string | null };
  data: FeatureCollection;
  loading: boolean;
  error: string | null;
  sidebar?: ReactNode;
}) {
  const router = useRouter();
  const countryLayer: FillLayer = {
    id: "country",
    source: "campaigns",
    type: "fill",
    paint: {
      "fill-color": "#020C7C",
      "fill-opacity": 0.6,
    },
    filter: ["all", ["==", "type", "country"]],
  };
  const regLayer: FillLayer = {
    id: "reg",
    source: "campaigns",
    type: "fill",
    paint: {
      "fill-color": "#020C7C",
      "fill-opacity": 0.6,
    },
    filter: ["all", ["==", "type", "reg"]],
  };
  const depLayer: FillLayer = {
    id: "dep",
    source: "campaigns",
    type: "fill",
    paint: {
      "fill-color": "#020C7C",
      "fill-opacity": 0.6,
    },
    filter: ["all", ["==", "type", "dep"]],
  };
  const aomLayer: FillLayer = {
    id: "aom",
    source: "campaigns",
    type: "fill",
    paint: {
      "fill-color": "#020C7C",
      "fill-opacity": 0.6,
    },
    filter: ["all", ["==", "type", "aom"]],
  };
  const epciLayer: FillLayer = {
    id: "epci",
    source: "campaigns",
    type: "fill",
    paint: {
      "fill-color": "#020C7C",
      "fill-opacity": 0.6,
    },
    filter: ["all", ["==", "type", "epci"]],
  };

  const mapStyle = Config.get<string>("observatoire.mapStyle");

  const bounds = useMemo(() => {
    const bounds =
      params.code === "XXXXX" ? [-5.225, 41.333, 9.55, 51.2] : bbox(data);
    return bounds as LngLatBoundsLike;
  }, [params.code, data]);

  const [hoverInfo, setHoverInfo] = useState<
    | {
        longitude: number;
        latitude: number;
        properties: any;
      }
    | undefined
  >();
  const [cursor, setCursor] = useState<string>("");
  const onMouseEnter = useCallback((e: any) => {
    setCursor("pointer");
    setHoverInfo({
      longitude: e.lngLat.lng,
      latitude: e.lngLat.lat,
      properties: e.features[0].properties,
    });
  }, []);
  const onMouseLeave = useCallback(() => {
    setCursor("");
    setHoverInfo(undefined);
  }, []);
  const onMouseMove = useCallback((e: MapLayerMouseEvent) => {
    e.features && e.features.length > 0
      ? setHoverInfo({
          longitude: e.lngLat.lng,
          latitude: e.lngLat.lat,
          properties: e.features[0].properties,
        })
      : undefined;
  }, []);
  const onClick = useCallback((e: MapLayerMouseEvent) => {
    e.features && e.features.length > 0
      ? router.push(
          getUrl("campagnes-incitation", {
            territory: e.features[0].properties.code,
            l_territory: e.features[0].properties.name,
            type: e.features[0].properties.type,
          })
        )
      : undefined;
  }, []);

  return (
    <>
      {loading && (
        <div className={fr.cx("fr-callout")}>
          <div>Chargement en cours...</div>
        </div>
      )}
      {error && (
        <div className={fr.cx("fr-callout")}>
          <div>{`Un problème est survenu au chargement des données: ${error}`}</div>
        </div>
      )}
      {!loading && !error && data.features.length === 0 && (
        <div className={fr.cx("fr-callout")}>
          <div>{`Ce territoire n'a pas encore réalisé de campagne d'incitation au covoiturage quotidien`}</div>
        </div>
      )}
      {!loading && !error && data.features.length > 0 && (
        <AppMap
          mapStyle={mapStyle}
          bounds={bounds}
          scrollZoom={false}
          interactiveLayerIds={["country", "reg", "dep", "aom", "epci"]}
          cursor={cursor}
          onMouseEnter={onMouseEnter}
          onMouseLeave={onMouseLeave}
          onMouseMove={onMouseMove}
          onClick={onClick}
          sidebar={sidebar}
          sidebarPosition="right"
          sidebarWidth={4}
          download={
            <DownloadButton
              title={"Télécharger les données de la carte"}
              data={data}
              type={"geojson"}
              filename="occupation"
            />
          }
        >
          <Source id="campaigns" type="geojson" data={data}>
            <Layer {...countryLayer} />
            <Layer {...regLayer} />
            <Layer {...depLayer} />
            <Layer {...aomLayer} />
            <Layer {...epciLayer} />
            <Popup
              longitude={hoverInfo ? hoverInfo.longitude : 0}
              latitude={hoverInfo ? hoverInfo.latitude : 0}
              closeButton={false}
            >
              {hoverInfo && (
                <div>
                  {hoverInfo.properties.name && (
                    <p>
                      <b>{hoverInfo.properties.name}</b>
                    </p>
                  )}
                  {hoverInfo.properties.fin && hoverInfo.properties.debut && (
                    <p>
                      Campagne active du{" "}
                      <b>
                        {new Date(
                          hoverInfo.properties.debut
                        ).toLocaleDateString()}{" "}
                        au{" "}
                        {new Date(
                          hoverInfo.properties.fin
                        ).toLocaleDateString()}
                      </b>
                    </p>
                  )}
                  {hoverInfo.properties.budget && (
                    <p>
                      Budget de la campagne :{" "}
                      <b>{hoverInfo.properties.budget} €</b>
                    </p>
                  )}
                </div>
              )}
            </Popup>
          </Source>
        </AppMap>
      )}
    </>
  );
}
