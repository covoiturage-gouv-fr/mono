import PageTitle from "@/components/common/PageTitle";
import { fr } from "@codegouvfr/react-dsfr";
import RessourceCard from "@/components/ressources/RessourceCard";
import { fetchAPI, cmsRessourcesByPage } from "@/helpers/cms";
import Pagination from "@/components/common/Pagination";
import { Metadata } from 'next';
import CategoryTags from '@/components/common/CategoryTags';

export const metadata: Metadata = {
  title: 'Ressources | Observatoire.covoiturage.gouv.fr',
  description: 'Toutes les ressources sur le covoiturage de courte distance',
}

export default async function Ressources() {
  const query = {
    populate: 'img,file',
    sort:'public_date:desc',
    pagination: {
      pageSize: cmsRessourcesByPage
    }
  };
  const { data, meta } = await fetchAPI('/resources',query);
  const pageTitle= 'Ressources'; 
  const nbPage = meta ? meta.pagination.pageCount : 1;
  const catQuery = {
    filters:{
      resources:{
        id:{
          $notNull: true
        }
      }
    }
  }
  const categories =  await fetchAPI('/categories',catQuery);

  return (
    <div id='content'>
      <PageTitle title={pageTitle} />
      <div className={fr.cx('fr-grid-row','fr-mb-3w')}>
        {categories.data && <CategoryTags categories={categories.data} type={'ressources'} />}
      </div>
      <div className={fr.cx('fr-grid-row', 'fr-grid-row--gutters')}>
        {data &&
          data.map((a:any, i:number) => {
            return (
              <div key={i} className={fr.cx('fr-col-12','fr-col-md-6')}>
                <RessourceCard 
                  title={a.attributes.title}
                  content={a.attributes.content}
                  date={new Date(a.attributes.public_date).toLocaleDateString('fr-FR')}
                  href={`/ressources/${a.attributes.slug}`}
                  img={a.attributes.img.data ? a.attributes.img.data.attributes.url : null}          
                />
              </div>
            )
          })
        }
      </div>
      <Pagination
        count={nbPage}
        href={`/ressources`}
      />    
    </div>
  );
}
