'use client'
import { searchTerritories, TerritorySearchResult } from '@/helpers/api';
import { targetMillesime } from '@/helpers/lists';
import { castPerimeterType, getUrl } from '@/helpers/search';
import { fr } from '@codegouvfr/react-dsfr';
import Tag from '@codegouvfr/react-dsfr/Tag';
import Autocomplete from '@mui/material/Autocomplete';
import { debounce } from '@mui/material/utils';
import TextField from '@mui/material/TextField';
import { useRouter } from 'next/navigation';
import { useEffect, useMemo, useRef, useState } from 'react';
import { useDashboardContext } from '../../context/DashboardProvider';

export default function SelectTerritory(props: { url:string }) {
  const { dashboard } = useDashboardContext();
  const router = useRouter();
  const defaultOption: TerritorySearchResult = {
    id: `${dashboard.params.code}_${dashboard.params.type}`,
    territory: dashboard.params.code,
    l_territory: dashboard.params.name,
    type: dashboard.params.type,
    year: dashboard.params.year
  }
  const [options, setOptions] = useState<TerritorySearchResult[]>([defaultOption]);
  const { year } = dashboard.params;
  // Une réponse lente ne doit pas écraser celle d'une frappe plus récente :
  // chaque recherche annule la précédente.
  const pending = useRef<AbortController>(undefined);
  const search = useMemo(
    () =>
      // eslint-disable-next-line react-hooks/refs -- la ref n'est lue qu'au déclenchement du debounce, pas au rendu
      debounce((v: string | null) => {
        pending.current?.abort();
        const { signal } = (pending.current = new AbortController());
        void searchTerritories(v, 20, targetMillesime(year), signal).then((results) => {
          if (!signal.aborted) setOptions(results);
        });
      }, 300),
    [year],
  );
  // Changement d'année ou démontage : le `search` précédent est remplacé, son
  // timer tirerait sur l'ancien millésime.
  useEffect(() => () => {
    search.clear();
    pending.current?.abort();
  }, [search]);

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
      onInputChange={(e, v, reason) => {
        // `reset` = libellé réinjecté après sélection, pas une saisie : ne pas rechercher.
        if (reason !== 'input') return;
        // Champ vidé : on rend la main au territoire courant plutôt qu'à une liste vide.
        if (!v.trim()) {
          search.clear();
          pending.current?.abort();
          setOptions([defaultOption]);
          return;
        }
        search(v);
      }}
      
      onChange={(e,v) =>{
        router.push(getUrl(props.url, v!))
        }
      }
    />
    </>
  );
}
