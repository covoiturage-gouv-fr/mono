'use client'
import { castPerimeterType, getUrl } from '@/helpers/search';
import type { CampaignDataInterface } from '@/interfaces/observatoire/dataInterfaces';
import { PerimeterType } from '@/interfaces/observatoire/Perimeter';
import { fr } from '@codegouvfr/react-dsfr';
import Autocomplete from '@mui/material/Autocomplete';
import TextField from '@mui/material/TextField';
import { useRouter } from 'next/navigation';
import { useState } from 'react';
import { useDashboardContext } from '../../context/DashboardProvider';

type IncentiveTerritoryOption = Pick<CampaignDataInterface, 'code' | 'type' | 'libelle'>;

export default function SelectIncentiveTerritory(props: { url: string, data:CampaignDataInterface[] }) {
  const { dashboard } = useDashboardContext();
  const router = useRouter();
  const defaultOption: IncentiveTerritoryOption[] = [props.data,{
    code: dashboard.params.code,
    libelle: dashboard.params.name,
    type: dashboard.params.type
  }].flat()
  const [options, setOptions] = useState<IncentiveTerritoryOption[]>(defaultOption);
  const search = (v: string | null) => {
    const filter = v ?  props.data.filter(d=> d.libelle?.toLowerCase().includes(v.toLowerCase())) : props.data
    setOptions(filter);
  };

  return ( 
    <>
    <Autocomplete
      id='select-territory'
      options={options}
      getOptionLabel={(option) => `${option.libelle ?? option.code} - ${castPerimeterType(option.type)}`}
      groupBy={(option) => option.type}
      renderGroup={(params) => (
        <li key={params.key}>
          <div className={fr.cx('fr-tag','fr-ml-3v')}>{castPerimeterType(params.group as PerimeterType)}</div>
          {params.children}
        </li>
      )}
      renderOption={(props, option) => {
        return (
          <li {...props} key={`${option.type}-${option.code}`}>
            <div>
              <div> <span className={fr.cx('fr-text--bold')}>{option.libelle}</span> <span className={fr.cx('fr-text--xs')}>({option.code})</span></div>              
            </div>
          </li>
        )
      }}
      noOptionsText={'Pas de résultats'}
      renderInput={(params) => <TextField {...params} label='Chercher mon territoire' />}
      filterOptions={(options) => options}
      onInputChange={(e, v) => {
        search(v);
      }}
      
      onChange={(e,v) =>{
        if (!v) return;
        router.push(getUrl(props.url, {territory: v.code, l_territory: v.libelle ?? '', type: v.type}))
        }
      }
    />
    </>
  );
}
