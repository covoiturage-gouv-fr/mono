import ActuCard from "@/components/actualites/ActuCard";
import CategoryTags from "@/components/common/CategoryTags";
import PageTitle from "@/components/common/PageTitle";
import Pagination from "@/components/common/Pagination";
import { cmsActusByPage, fetchAPI, shorten } from "@/helpers/cms";
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
    title: `Catégorie ${data ? data[0].attributes.label : ''} des actualités | Observatoire.covoiturage.gouv.fr`,
    description: `Catégorie ${data ? data[0].attributes.label : ''} des actualités sur le covoiturage de courte distance`,
  }
}

export async function generateStaticParams() {
  const query = {
    fields: 'slug',
  };
  const {data} =  await fetchAPI('/categories',query);
  
  return data.map((post:any) => ({
    slug: post.attributes.slug,
  }))
}

export default async function ActuCategoryPage({ params }: { params: Promise<{ slug: string }>}) {
  const { slug } = await params;
  const query = {
    populate: 'categories,img',
    filters:{
      categories:{
        slug:{
          $eq: slug
        }
      }
    },
    sort:'createdAt:desc',
    pagination: {
      pageSize: cmsActusByPage,
    }
  };
  const { data, meta }  = await fetchAPI('/articles',query);
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
  const pageTitle = 'Actualités';
  const nbPage = meta.pagination.pageCount;

  return (
    <div id='content'>
      <PageTitle title={pageTitle} />
      <div className={fr.cx('fr-grid-row','fr-mb-3w')}>
        {categories.data && <CategoryTags categories={categories.data} active={slug} />}
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
                  img={a.attributes.img.data.attributes.formats.medium ? a.attributes.img.data.attributes.formats.medium.url : a.attributes.img.data.attributes.url}
                  img_legend={a.attributes.img_legend}
                  categories={a.attributes.categories.data}
                />
              </div>
            )
          })
        }
      </div>
      <Pagination
        count={nbPage}
        href={`/actualites/categorie/${slug}`}
      />     
    </div>
  );
}
