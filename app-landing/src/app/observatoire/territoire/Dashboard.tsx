'use client'
import SectionTitle from '@/components/common/SectionTitle';
import SelectInList from '@/components/common/SelectInList';
import SelectMonth from '@/components/observatoire/SelectMonth';
import SelectObserve from '@/components/observatoire/SelectObserve';
import SelectPeriod from '@/components/observatoire/SelectPeriod';
import SelectSemester from '@/components/observatoire/SelectSemester';
import SelectTerritory from '@/components/observatoire/SelectTerritory';
import SelectTrimester from '@/components/observatoire/SelectTrimester';
import SelectYear from '@/components/observatoire/SelectYear';
import { useDashboardContext } from '@/context/DashboardProvider';
import { GetPeriod } from '@/helpers/dashboard';
import { graphList, mapList } from '@/helpers/lists';
import { PerimeterType } from '@/interfaces/observatoire/Perimeter';
import { fr } from '@codegouvfr/react-dsfr';
import CircularProgress from '@mui/material/CircularProgress';
import { useSearchParams } from 'next/navigation';
import { useEffect } from 'react';
import DistanceGraph from './graphs/DistanceGraph';
import FluxGraph from './graphs/FluxGraph';
import IncentiveGraph from './graphs/IncentiveGraph';
import OccupationGraph from './graphs/OccupationGraph';
import RepartitionDistanceGraph from './graphs/RepartitionDistanceGraph';
import RepartitionHoraireGraph from './graphs/RepartitionHoraireGraph';
import KeyFigures from './KeyFigures';
import AiresCovoiturageMap from './maps/AiresMap';
import DensiteMap from './maps/DensiteMap';
import FluxMap from './maps/FluxMap';
import OccupationMap from './maps/OccupationMap';
import BestFluxTable from './tables/BestFluxTable';
import BestTerritoriesTable from './tables/BestTerritoriesTable';

export default function Dashboard() {
  const searchParams = useSearchParams();
  const {dashboard} =useDashboardContext();
  const period = GetPeriod();
  const observeLabel = dashboard.params.map == 1 ? 'Flux entre:' : 'Territoires observés';
  useEffect(() => {
    const params = {
      code: searchParams.get('code') ? searchParams.get('code')! : 'XXXXX',
      type: searchParams.get('type') ? searchParams.get('type')! as PerimeterType : 'country'
    }
    dashboard.onLoadTerritory(params);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchParams.get('code'), searchParams.get('type')]);

  return(
    <>
      <div className={fr.cx('fr-grid-row','fr-grid-row--gutters','fr-mt-5v')}>
        <div className={fr.cx('fr-col-12','fr-col-md-6')}>
          <SelectTerritory url={'territoire'} />
        </div>
        <div className={fr.cx('fr-col-12','fr-col-md-6')}>
          <SelectPeriod id='period' label='Type de période' />
          {dashboard.params.period === 'month' && 
            <SelectMonth />
          }
          {dashboard.params.period === 'trimester' && 
            <SelectTrimester />
          }
          {dashboard.params.period === 'semester' && 
            <SelectSemester />
          }
          <SelectYear />
        </div>
      </div>
      {dashboard.loading ? (
        <div className={fr.cx('fr-grid-row','fr-grid-row--gutters')}>
          <div className={fr.cx('fr-mx-auto','fr-py-10w')}><CircularProgress /></div>
        </div>
      ) 
      : (
        <>
          <SectionTitle
            title={`${dashboard.params.name} du ${new Date(period.start_date).toLocaleDateString()} au ${new Date(
              period.end_date,
            ).toLocaleDateString()}`}
          />
          <KeyFigures />
          <div className={fr.cx('fr-grid-row', 'fr-grid-row--gutters')}>
            <div className={fr.cx('fr-col-12','fr-col-md-6')}>
              <RepartitionHoraireGraph title='Trajets par horaire' />
            </div>
            <div className={fr.cx('fr-col-12','fr-col-md-6')}>
              <RepartitionDistanceGraph title='Répartition des trajets par distance' />
            </div>
          </div>
          <div className={fr.cx('fr-grid-row', 'fr-grid-row--gutters')}>
            <div className={fr.cx('fr-col-12','fr-col-md-6')}>
              <BestFluxTable title='Top 10 des trajets les plus covoiturés' limit={10} />
            </div>
            <div className={fr.cx('fr-col-12','fr-col-md-6')}>
              <BestTerritoriesTable title='Top 10 des territoires' limit={10} />
            </div>
          </div>
          <div className={fr.cx('fr-grid-row', 'fr-grid-row--gutters')}>
            <div className={fr.cx('fr-col-12','fr-col-md-6')}>
              <IncentiveGraph title="Répartition des incitations par types d'incitateurs" />
            </div>
          </div>
          <SectionTitle title='Evolution' />
          <div className={fr.cx('fr-grid-row', 'fr-grid-row--gutters')}>
            <div className={fr.cx('fr-col-12')}>
              <SelectInList
                labelId='graph'
                label='Sélectionner un graphique'
                id={dashboard.params.graph}
                list={graphList}
                sx={{ minWidth: 300 }}
                onChange={dashboard.onChangeGraph}
              />
            </div>
          </div>
          <div className={fr.cx('fr-grid-row', 'fr-grid-row--gutters')}>
            <div className={fr.cx('fr-col-12')}>
              {dashboard.params.graph == 1 && <FluxGraph title={graphList[0].name} indic="journeys"/>}
              {dashboard.params.graph == 2 && <DistanceGraph title={graphList[1].name} />}
              {dashboard.params.graph == 3 && <OccupationGraph title={graphList[2].name} indic="occupation_rate"/>}
            </div>
          </div>
          <SectionTitle title='Cartographie' />
          <div className={fr.cx('fr-grid-row', 'fr-grid-row--gutters')}>
            <div className={fr.cx('fr-col-12','fr-col-md-6')}>
              <SelectInList
                labelId='carte'
                label='Sélectionner une carte'
                id={dashboard.params.map}
                list={dashboard.params.code=='XXXXX' ? mapList.filter( m => m.id !== 2) : mapList}
                sx={{ minWidth: 300 }}
                onChange={dashboard.onChangeMap}
              />
            </div>
            {[1,3].includes(dashboard.params.map) && 
              <div className={fr.cx('fr-col-12','fr-col-md-6')}>
                <SelectObserve id='observe' label={observeLabel} />
              </div>
            }
          </div>
          <div className={fr.cx('fr-grid-row', 'fr-grid-row--gutters')}>
            <div className={fr.cx('fr-col-12')}>
              {dashboard.params.map == 1 && <FluxMap title={mapList[0].name} />}
              {dashboard.params.map == 2 && <DensiteMap title={mapList[1].name} />}
              {dashboard.params.map == 3 && <OccupationMap title={mapList[2].name} />}
              {dashboard.params.map == 4 && <AiresCovoiturageMap title={mapList[3].name} />}
            </div>
          </div>
        </>
      )} 
    </>
  );
}