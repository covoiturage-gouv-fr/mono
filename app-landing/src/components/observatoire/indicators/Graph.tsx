'use client'
import FluxGraph from '@/app/observatoire/territoire/graphs/FluxGraph';
import DistanceGraph from '@/app/observatoire/territoire/graphs/DistanceGraph';
import { SingleGraphProps } from '@/interfaces/observatoire/componentsInterfaces';
import OccupationGraph from '@/app/observatoire/territoire/graphs/OccupationGraph';


export default function SingleGraph(props: SingleGraphProps) {
  return(
    <>
      {props.params.graph == 1 && <FluxGraph title={props.title} indic="journeys" />}
      {props.params.graph== 2 && <DistanceGraph title={props.title} />}
      {props.params.graph== 3 && <OccupationGraph title={props.title} indic="occupation_rate" />}
      {props.params.graph== 4 && <OccupationGraph title={props.title} indic="trips" />}
    </>
  )
};