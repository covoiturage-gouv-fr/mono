'use client'
import { castPerimeterType, getUrl } from '@/helpers/search';
import { PerimeterType } from '@/interfaces/observatoire/Perimeter';
import { fr } from '@codegouvfr/react-dsfr';
import Autocomplete from '@mui/material/Autocomplete';
import TextField from '@mui/material/TextField';
import { useRouter } from 'next/navigation';
import { useState } from 'react';
import { useDashboardContext } from '../../context/DashboardProvider';

export default function SelectIncentiveTerritory(props: { url: string, data:any[] }) {
  const { dashboard } = useDashboardContext();
  const router = useRouter();
  const defaultOption = [props.data,{
    code: dashboard.params.code,
    collectivite: dashboard.params.name,
    type: dashboard.params.type
  }].flat()
  const [options, setOptions] = useState<any[]>(defaultOption);
  const search =  async (v: string | null) => {
    const filter = v ?  props.data.filter(d=> d.collectivite.toLowerCase().includes(v.toLowerCase())) : props.data
    setOptions(filter);
  };

  return ( 
    <>
    <Autocomplete
      id='select-territory'
      options={options}
      getOptionLabel={(option) => `${option.l_territory} - ${castPerimeterType(option.type)}`}
      groupBy={(option) => option.type}
      renderGroup={(params) => (
        <li key={params.key}>
          <div className={fr.cx('fr-tag','fr-ml-3v')}>{castPerimeterType(params.group as PerimeterType)}</div>
          {params.children}
        </li>
      )}
      renderOption={(props, option) => {
        return (
          <li {...props} key={option.id}>
            <div>
              <div> <span className={fr.cx('fr-text--bold')}>{option.collectivite}</span> <span className={fr.cx('fr-text--xs')}>({option.code})</span></div>              
            </div>
          </li>
        )
      }}
      noOptionsText={'Pas de résultats'}
      renderInput={(params) => <TextField {...params} label='Chercher mon territoire' />}
      filterOptions={(options) => options}
      onInputChange={async(e, v) => {
        await search(v);
      }}
      
      onChange={(e,v) =>{
        router.push(getUrl(props.url, {territory: v.code, l_territory: v.collectivite, type: v.type}))
        }
      }
    />
    </>
  );
}
