import ActuCard from "@/components/actualites/ActuCard";
import CategoryTags from "@/components/common/CategoryTags";
import PageTitle from "@/components/common/PageTitle";
import Pagination from "@/components/common/Pagination";
import { cmsActusByPage, fetchAPI, shorten } from "@/helpers/cms";
import { fr } from "@codegouvfr/react-dsfr";
import { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'Actualités | Observatoire.covoiturage.gouv.fr',
  description: 'Toutes les actualités sur le covoiturage de courte distance',
}

export default async function Actualites() {
  const query = {
    populate: 'img',
    sort:'createdAt:desc',
    pagination: {
      pageSize: cmsActusByPage
    }
  };
  const { data, meta }  = await fetchAPI('/articles',query);
  const pageTitle = 'Actualités';
  const nbPage = meta ? meta.pagination.pageCount : 1;
  const catQuery = {
    filters:{
      articles:{
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
        {categories.data && <CategoryTags categories={categories.data} />}
      </div>
      <div className={fr.cx('fr-grid-row', 'fr-grid-row--gutters')}>
        {data &&
          data.map((a:any, i:number) => {
            return (
              <div key={i} className={fr.cx('fr-col-12','fr-col-md-6')}>
                <ActuCard 
                  title={a.attributes.title}
                  content={shorten(a.attributes.description,250)}
                  date={new Date(a.attributes.createdAt).toLocaleDateString('fr-FR')}
                  href={`/actualites/${a.attributes.slug}`}
                  img={a.attributes.img.data.attributes.url}
                  img_legend={a.attributes.img_legend}
                  categories={a.attributes.categories}
                />
              </div>
            )
          })
        }
      </div>
      <Pagination
        count={nbPage}
        href={`/actualites`}
      />   
    </div>
  );
}
