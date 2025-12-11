'use client'
//import DensiteMap from '@/app/observatoire/territoire/maps/DensiteMap';
//import FluxMap from '@/app/observatoire/territoire/maps/FluxMap';
import OccupationMap from '@/app/observatoire/territoire/maps/OccupationMap';
import AiresCovoiturageMap from '@/app/observatoire/territoire/maps/AiresMap';
import { SingleMapProps } from '@/interfaces/observatoire/componentsInterfaces';
import VrMap from '@/app/observatoire/territoire/maps/VrMap';

export default function SingleMap(props: SingleMapProps) {
  return(
    <>
      {props.params.map == 3 && <OccupationMap title={props.title} />}
      {props.params.map == 4 && <AiresCovoiturageMap title={props.title} />}
      {props.params.map == 5 && <VrMap title={props.title} />}
    </>
  )
};