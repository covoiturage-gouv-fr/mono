'use client'
import { castPerimeterType, fetchSearchAPI, getUrl } from '@/helpers/search';
import { fr } from '@codegouvfr/react-dsfr';
import Tag from '@codegouvfr/react-dsfr/Tag';
import Autocomplete from '@mui/material/Autocomplete';
import TextField from '@mui/material/TextField';
import { useRouter } from 'next/navigation';
import { useState } from 'react';
import { useDashboardContext } from '../../context/DashboardProvider';

export default function SelectTerritory(props: { url:string }) {
  const { dashboard } = useDashboardContext();
  const router = useRouter();
  const defaultOption = {
    territory: dashboard.params.code,
    l_territory: dashboard.params.name,
    type: dashboard.params.type
  }
  const [options, setOptions] = useState<any[]>([defaultOption]);
  const search =  async (v: string | null) => {
    const query = {
      q:v,
      attributesToSearchOn:['territory','l_territory'], 
      limit:20
    };
    const response = await fetchSearchAPI('indexes/geo/search',{method:'post',body: JSON.stringify(query)});
    setOptions(response.hits);
  };

  return ( 
    <>
    <Autocomplete
      id='select-territory'
      options={options}
      getOptionLabel={(option) => `${option.l_territory} - ${castPerimeterType(option.type)}`}
      renderOption={(props, option) => {
        return (
          <li {...props} key={option.id}>
            <div>
              <div> <span className={fr.cx('fr-text--bold')}>{option.l_territory}</span> <span className={fr.cx('fr-text--xs')}>({option.territory})</span></div>
              <div>
                <Tag small>{castPerimeterType(option.type)}</Tag>
              </div>
              
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
        router.push(getUrl(props.url, v!))
        }
      }
    />
    </>
  );
}
