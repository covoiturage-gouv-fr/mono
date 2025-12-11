
import PageTitle from '@/components/common/PageTitle';
import { fetchAPI } from "@/helpers/cms";
import { fr } from "@codegouvfr/react-dsfr";
import { Metadata } from 'next';
import Link from 'next/link';

export const metadata: Metadata = {
  title: 'Plan du site | Observatoire.covoiturage.gouv.fr',
  description: 'Plan du site Observatoire.covoiturage.gouv.fr',
}

export default async function Plan() {
  const query = {
    populate: {
      tags: {
        populate: 'slug'
      }
    },
    fields:['title','slug'],  
    sort:'createdAt',
    pagination:{
      limit: -1
    }
  };
  const {data}  = await fetchAPI('/pages',query);
  const home = data ? data.find((p:any) => p.attributes.tags.data[0].attributes.slug === 'accueil') : []
  const obs = data ? data.filter((p:any) => p.attributes.tags.data[0].attributes.slug === 'observatoire') : []
  const collectivites = data ? data.filter((p:any) => p.attributes.tags.data[0].attributes.slug === 'collectivites') : []
  const autresActeurs = data ? data.filter((p:any) => ['plateformes','covoitureurs','employeurs'].includes(p.attributes.tags.data[0].attributes.slug)) : []
  const communs = data ? data.filter((p:any)=> p.attributes.tags.data[0].attributes.slug === 'commun') : []
  const actus = await fetchAPI('/articles',{
    fields:['title','slug'],
    sort:'createdAt',
    pagination:{
      limit: -1
    }
  });
  const ressources = await fetchAPI('/resources',{
    fields:['title','slug'],
    sort:'createdAt',
    pagination:{
      limit: -1
    }
  });

  return (
    <div id='content'>
      <PageTitle title='Plan du site' />
      <div className={fr.cx('fr-grid-row')}>
        <div className={fr.cx('fr-col')}>
          {home && 
            <ul>
              <li>
                <Link className={fr.cx('fr-link')}  href="/" target="_self">{home.attributes.title}</Link>
              </li>
            </ul>
          }
          {obs &&
            <>
              <h2 className={fr.cx('fr-h3','fr-mb-0')}>Observatoire</h2> 
              <ul>
                {obs.map((p:any, i:number)=>
                  <li key={i}> 
                    <Link className={fr.cx('fr-link')}href={`/observatoire/${p.attributes.slug}`} target="_self">
                      {p.attributes.title}
                    </Link>
                  </li>
                )}
                <li>
                  <Link className={fr.cx('fr-link')}href={`/observatoire/territoire`} target="_self">
                    Observer un territoire
                  </Link>
                </li>
              </ul>
            </>
          }
          {collectivites &&
            <>
              <h2 className={fr.cx('fr-h3','fr-mb-0')}>Collectivités</h2> 
              <ul>
                {collectivites.map((p:any, i:number)=>
                  <li key={i}> 
                    <Link className={fr.cx('fr-link')}href={`/collectivites/${p.attributes.slug}`} target="_self">
                      {p.attributes.title}
                    </Link>
                  </li>
                )}
              </ul>
            </>
          }
          {autresActeurs &&
            <>
              <h2 className={fr.cx('fr-h3','fr-mb-0')}>Autres acteurs</h2> 
              <ul>
                {autresActeurs.map((p:any, i:number)=>
                  <li key={i}> 
                    <Link className={fr.cx('fr-link')}href={`/autres-acteurs/${p.attributes.slug}`} target="_self">
                      {p.attributes.title}
                    </Link>
                  </li>
                )}
              </ul>
            </>
          }
          {actus.data &&
            <>
              <h2 className={fr.cx('fr-h3','fr-mb-0')}>
                <Link href={`/actualites`} target="_self">
                  Actualités
                </Link>
              </h2> 
              <ul>
                {actus.data.map((p:any, i:number)=>
                  <li key={i}> 
                    <Link className={fr.cx('fr-link')}href={`/actualites/${p.attributes.slug}`} target="_self">
                      {p.attributes.title}
                    </Link>
                  </li>
                )}
              </ul>
            </>
          }
          {ressources.data &&
            <>
              <h2 className={fr.cx('fr-h3','fr-mb-0')}>
                <Link href={`/ressources`} target="_self">
                  Ressources
                </Link>
              </h2> 
              <ul>
                {ressources.data.map((p:any, i:number)=>
                  <li key={i}> 
                    <Link className={fr.cx('fr-link')}href={`/ressources/${p.attributes.slug}`} target="_self">
                      {p.attributes.title}
                    </Link>
                  </li>
                )}
              </ul>
            </>
          }
           {communs &&
            <>
              <h2 className={fr.cx('fr-h3','fr-mb-0')}>Autres pages</h2> 
              <ul>
                {communs.map((p:any, i:number)=>
                  <li key={i}> 
                    <Link className={fr.cx('fr-link')}href={`/${p.attributes.slug}`} target="_self">
                      {p.attributes.title}
                    </Link>
                  </li>
                )}
              </ul>
            </>
          }
        </div>
      </div>
    </div>
  );
}
