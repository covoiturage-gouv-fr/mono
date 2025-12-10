'use client'
import { Config } from '@/config';
import { useApi } from '@/hooks/useApi';
import { PerimeterType } from '@/interfaces/observatoire/Perimeter';
import { fr } from '@codegouvfr/react-dsfr';
import CircularProgress from '@mui/material/CircularProgress';
import { feature, featureCollection } from '@turf/helpers';
import { FeatureCollection } from 'geojson';
import { useSearchParams } from 'next/navigation';
import { useMemo } from 'react';
import SelectIncentiveTerritory from '../../../components/observatoire/SelectIncentiveTerritory';
import SelectIncentiveYear from '../../../components/observatoire/SelectIncentiveYear';
import Details from './Details';
import IncentiveMap from './maps/IncentiveMap';


export default function Dashboard() {
  const searchParams = useSearchParams();
  const params = {
    code: searchParams.get('code') ? searchParams.get('code')! : 'XXXXX',
    type: searchParams.get('type') ? searchParams.get('type')! as PerimeterType : 'country',
    year: searchParams.get('year') ? searchParams.get('year') : null
  }
  const apiUrl = Config.get<string>('next.public_api_url', '');
  const url = `${apiUrl}/campaigns${params.code !== 'XXXXX' 
    ?`?code=${params.code}&type=${params.type}${params.year ? `&year=${params.year}` : ''}`
    : params.year ? `?year=${params.year}` : ''}`;
  const { data, error, loading } = useApi<any[]>(url);
  const geojson = useMemo(() => {
    const campaignsData = data ? data : [];
    return featureCollection(
      campaignsData.map((d) =>
        feature(d.geom, {
          name:d.collectivite,
          type: d.type,
          code: d.code,
          debut: d.date_debut,
          fin: d.date_fin,
          budget: d.budget_incitations,
          operateurs: d.operateurs,
          p_gratuit: d.passager_gratuite,
          p_eligible: d.passager_eligible_gratuite,
          p_t_max: d.passager_trajets_max_par_mois,
          p_tk: d.passager_reduction_ticket,
          p_e_tk: d.passager_eligibilite_reduction,
          p_m_tk: d.passager_montant_ticket,
          p_max: d.passager_trajets_max_par_mois,
          c_m_max_p: d.conducteur_montant_max_par_passager,
          c_m_max: d.conducteur_montant_max_par_mois,
          c_m_min_p: d.conducteur_montant_min_par_passager,
          c_t_max: d.conducteur_trajets_max_par_mois,
          sens: d.zone_sens_des_trajets,
          exclusion: d.zone_exclusion,
          liste_exclusion: d.si_zone_exclue_liste,
          autre_exclusion: d.autre_exclusion,
          l_min: d.trajet_longueur_min,
          l_max: d.trajet_longueur_max,
          classe: d.trajet_classe_de_preuve,
          lien: d.lien,
        }),
      ),
    ) as FeatureCollection;
  }, [data]);

  return(
    <>
      {loading ? (
        <div className={fr.cx('fr-grid-row','fr-grid-row--gutters')}>
          <div className={fr.cx('fr-mx-auto','fr-py-10w')}><CircularProgress /></div>
        </div>
      ) 
      : (
        <>
          <div className={fr.cx('fr-grid-row', 'fr-grid-row--gutters')}>
            <div className={fr.cx('fr-col-12')}>
              <IncentiveMap params={params} 
                data={geojson} 
                loading={loading} 
                error={error} 
                sidebar={<>
                  {geojson.features.length > 1 ? <div className={fr.cx('fr-h4')}>{geojson.features.length} campagnes</div> : ''}
                  <SelectIncentiveYear params={params} url={'campagnes-incitation'}/>
                  <SelectIncentiveTerritory url={'campagnes-incitation'} data={data!}/>
                </>}
              />
            </div>
          </div>
          { params.code !== 'XXXXX' && geojson.features.length > 0 &&
          <div className={fr.cx('fr-grid-row', 'fr-grid-row--gutters')}>
            <div className={fr.cx('fr-col-12')}>
             <Details data={geojson.features[0].properties} />
            </div>
          </div>
          }
        </>
      )} 
    </>
  );
}