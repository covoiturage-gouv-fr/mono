import CategoryTags from "@/components/common/CategoryTags";
import PageTitle from "@/components/common/PageTitle";
import Pagination from "@/components/common/Pagination";
import RessourceCard from '@/components/ressources/RessourceCard';
import { cmsRessourcesByPage, fetchAPI } from "@/helpers/cms";
import { fr } from "@codegouvfr/react-dsfr";

export async function generateMetadata({ params }: { params: Promise<{ slug: string }>}) {
  const { slug } = await params;
  const query = {
    filters: {
      slug: {
        $eq: slug,
      }
    }
  };
  const {data} =  await fetchAPI('/categories',query);
  return {
    title: `Catégorie ${data ? data[0].attributes.label : ''} des ressources | Observatoire.covoiturage.gouv.fr`,
    description: `Catégorie ${data ? data[0].attributes.label : ''} des ressources sur le covoiturage de courte distance`,
  }
}

export async function generateStaticParams() {
  const query = {
    fields: 'slug',
    pagination: {
      limit:-1,
    },
  };
  const {data} =  await fetchAPI('/categories',query);
  
  return data.map((post:any) => ({
    slug: post.attributes.slug,
  }))
}

export default async function ResourceCategoryPage({ params }: { params: Promise<{ slug: string }>}) {
  const { slug } = await params;
  const query = {
    populate: 'categories,img,file',
    filters:{
      categories:{
        slug:{
          $eq: slug
        }
      }
    },
    sort:'public_date:desc',
    pagination: {
      pageSize: cmsRessourcesByPage,
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
  const pageTitle = 'Ressources';
  const nbPage = meta.pagination.pageCount;

  return (
    <div id='content'>
      <PageTitle title={pageTitle} />
      <div className={fr.cx('fr-grid-row','fr-mb-3w')}>
        {categories.data && <CategoryTags categories={categories.data} active={slug} type={'ressources'} />}
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
        href={`/ressources/categorie/${slug}`}
      />     
    </div>
  );
}
