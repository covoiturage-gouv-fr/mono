import PageTitle from "@/components/common/PageTitle";
import Pagination from "@/components/common/Pagination";
import RessourceCard from "@/components/ressources/RessourceCard";
import { cmsRessourcesByPage, fetchAPI } from "@/helpers/cms";
import { fr } from "@codegouvfr/react-dsfr";
import CategoryTags from '../../../../components/common/CategoryTags';

export async function generateMetadata({ params }: { params: Promise<{ id: string }>}) {
  const { id } = await params;
  return {
    title: `Ressources page ${id} | Observatoire.covoiturage.gouv.fr`,
    description: `Page ${id} des ressources sur le covoiturage de courte distance`,
  }
}

export async function generateStaticParams() {
  const query = {
    pagination: {
      pageSize: cmsRessourcesByPage
    }
  };
  const { meta }  = await fetchAPI('/resources',query);
  const nbPage = meta.pagination.pageCount;
  return Array.from({ length: nbPage }, (_, v) => {
    const id = v + 1;
    return {
      id: id.toString(),
    }
  });
}

export default async function RessourcePage({ params }: { params: Promise<{ id: string }>}) {
  const { id } = await params;
  const query = {
    populate: 'img,file',
    sort:'public_date:desc',
    pagination: {
      pageSize: cmsRessourcesByPage,
      page: id,
    }
  };
  const { data, meta }  = await fetchAPI('/resources',query);
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
  const nbPage = meta.pagination.pageCount;
  const pageTitle= `Ressources page ${id}`; 


  return (
    <div id='content'>
      <PageTitle title={pageTitle} />
      <div className={fr.cx('fr-grid-row','fr-mb-3w')}>
        {categories.data && <CategoryTags categories={categories.data} type={'ressources'} page={id}/>}
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
        defaultPage={Number(id)}
        href={`/ressources`}
      />    
    </div>
  );
}