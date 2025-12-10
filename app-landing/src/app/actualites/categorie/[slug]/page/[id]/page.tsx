import ActuCard from "@/components/actualites/ActuCard";
import CategoryTags from '@/components/common/CategoryTags';
import PageTitle from "@/components/common/PageTitle";
import Pagination from "@/components/common/Pagination";
import { cmsActusByPage, fetchAPI, shorten } from "@/helpers/cms";
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
    title: `Actualités page ${id} pour la catégorie ${data ? data[0].attributes.label : ''}| Observatoire.covoiturage.gouv.fr`,
    description: `Page ${id} des actualités sur le covoiturage de courte distance pour la catégorie ${data ? data[0].attributes.label : ''}`,
  }
}

export async function generateStaticParams() {
  const categories =  await fetchAPI('/categories',{
    fields: 'slug',
    filters:{
      articles:{
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
        pageSize: cmsActusByPage
      }
    };
    const { meta }  = await fetchAPI('/articles',query);
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

export default async function ActuCategoriePage({ params }: { params: Promise<{ slug: string, id: string }>}) {
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
      pageSize: cmsActusByPage,
      page: id,
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
  const nbPage = meta ? meta.pagination.pageCount : 1;
  const pageTitle = `Actualités de la catégorie ${categories.data.find((c:any) => c.attributes.slug === slug).attributes.label} page ${id}`; 

  return (
    <div id='content'>
      <PageTitle title={pageTitle} />
      <div className={fr.cx('fr-grid-row','fr-mb-3w')}>
        {categories.data && <CategoryTags categories={categories.data} active={slug} page={id}/>}
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
        defaultPage={Number(id)}
        href={`/actualites`}
      />    
    </div>
  );
}