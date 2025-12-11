import CategoryTags from '@/components/common/CategoryTags';
import PageTitle from "@/components/common/PageTitle";
import Pagination from "@/components/common/Pagination";
import RessourceCard from "@/components/ressources/RessourceCard";
import { cmsRessourcesByPage, fetchAPI } from "@/helpers/cms";
import { fr } from "@codegouvfr/react-dsfr";

export async function generateMetadata({ params }: { params: Promise<{ slug: string, id: string }>}) {
  const { slug, id } = await params;
  const query = {
    filters: {
      slug: {
        $eq: slug,
      }
    }
  };
  const {data} =  await fetchAPI('/categories',query);  
  return {
    title: `Ressources page ${id} pour la catégorie ${data ? data[0].attributes.label : ''}| Observatoire.covoiturage.gouv.fr`,
    description: `Page ${id} des ressources sur le covoiturage de courte distance pour la catégorie ${data ? data[0].attributes.label : ''}`,
  }
}

export async function generateStaticParams() {
  const categories =  await fetchAPI('/categories',{
    fields: 'slug',
    filters:{
      resources:{
        id:{
          $notNull: true
        }
      }
    }
  });
  const data = await Promise.all(categories.data.map(async (c:any) => {
    const query = {
      filter:{
        categories:{
          slug:{
            $eq: c.attributes.slug
          }
        }
      },
      pagination: {
        pageSize: cmsRessourcesByPage
      }
    };
    const { meta }  = await fetchAPI('/resources',query);
    const nbPage = meta.pagination.pageCount;
    return Array.from({ length: nbPage }, (_, v) => {
      const id = v + 1;
      return {
        slug: c.attributes.slug,
        id: id.toString(),
      }
    })
  }))
  return data.flat()
}

export default async function RessourceCategoriePage({ params }: { params: Promise<{ slug: string, id: string }>}) {
  const { slug, id } = await params;
  const query = {
    populate: 'img,file',
    sort:'public_date:desc',
    filters:{
      categories:{
        slug:{
          $eq: slug
        }
      }
    },
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
  const nbPage = meta ? meta.pagination.pageCount : 1;
  const pageTitle = `Ressources de la catégorie ${categories.data.find((c:any) => c.attributes.slug === slug).attributes.label} page ${id}`; 

  return (
    <div id='content'>
      <PageTitle title={pageTitle} />
      <div className={fr.cx('fr-grid-row','fr-mb-3w')}>
        {categories.data && <CategoryTags categories={categories.data} active={slug} type={'ressources'} page={id}/>}
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