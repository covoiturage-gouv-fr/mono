'use client'
import Autocomplete from "@mui/material/Autocomplete";
import { cx } from "@codegouvfr/react-dsfr/tools/cx";
import { fr } from '@codegouvfr/react-dsfr';
import { fetchSearchAPI } from '@/helpers/search';
import { useState } from 'react';
import Badge from '@codegouvfr/react-dsfr/Badge';
import { useRouter } from 'next/navigation';

type SearchBarProps = {
  className?: string;
  id: string;
  placeholder: string;
  type: "search";
};

type SearchOptions = {
  id: ContentTypes,
  title: string,
  tag:string | null,
  slug: string,
  createdAt: string
};
type ContentTypes = 'article' | 'resource' | 'page';

export default function SearchBar(props: SearchBarProps) {
  const { className, id, placeholder, type } = props;
  const [options, setOptions] = useState<SearchOptions[]>([]);
  const search =  async (v: string | null) => {
    const query = {
      queries: [
        {q:v, indexUid:"article",attributesToRetrieve:["title","slug","content","createdAt"],limit:50},
        {q:v, indexUid:"resource",attributesToRetrieve:["title","slug","content","createdAt"],limit:50},
        {q:v, indexUid:"page",attributesToRetrieve:["title","slug","tags","content","createdAt"],limit:50}
      ]
    };
    const response = await fetchSearchAPI('multi-search',{method:'post',body: JSON.stringify(query)})
   setOptions(response.results.map((r:any) => r.hits.map( (h:any) => {
      // eslint-disable-next-line no-unused-vars
      const { tags,content, ...hit } = h;
      void content;
      void tags;
      return {
        id: r.indexUid,
        tag: h.tags ? h.tags[0].slug : null,
        ...hit,
      }
    })).flat());
  };
  const contentTypes = [
    {id: 'article', name:'Actualités', url:'/actualites'},
    {id: 'resource', name:'Ressource', url:'/ressources'},
    {id: 'page', name:'Page', url:''},
  ]

  const castType = (value: ContentTypes) => {
    return contentTypes.find(c => c.id === value)
  }
  const router = useRouter();

  const getUrl = (option:SearchOptions) => {
    const params = {...option}
    if(option.tag === 'commun'){
      params.tag = null
    }
    if(option.tag === 'accueil') {
      params.tag = null
      params.slug = ''
    }
    return `${castType(params.id)!.url}${params.tag ? `/${params.tag}`:''}/${params.slug}`
  }

  return (
   <Autocomplete 
      id='search'
      options={options}
      noOptionsText={'Pas de résultats'}
      getOptionLabel={(option) => option.title}
      renderOption={(props, option) => {
        return (
          <li {...props} key={option.slug}>
            <div key={option.slug}>
              <span>{option.title}</span>
              <span className={fr.cx('fr-px-2v')}>
                <Badge  severity="info" small>
                  {castType(option.id)!.name}
                </Badge>
              </span>
            </div>
          </li>
        )
      }}
      onInputChange={async(e, v) => {
        await search(v);
      }}
      renderInput={params => 
        <div ref={params.InputProps.ref}>
          <input 
            {...params.inputProps} 
            className={cx(params.inputProps.className, className)}
            id={id}
            placeholder={placeholder}
            type={type}
          />
        </div>
      }
      onChange={(e,v) =>{
        router.push(getUrl(v!))
        }
      }
      
    />
  );
}