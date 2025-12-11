import ActuCard from "@/components/actualites/ActuCard";
import CategoryTags from "@/components/common/CategoryTags";
import PageTitle from "@/components/common/PageTitle";
import Pagination from "@/components/common/Pagination";
import { cmsActusByPage, fetchAPI, shorten } from "@/helpers/cms";
import { fr } from "@codegouvfr/react-dsfr";

export async function generateMetadata({ params }: { params: Promise<{ id: string }>}) {
  const { id } = await params;
  return {
    title: `Actualités page ${id} | Observatoire.covoiturage.gouv.fr`,
    description: `Page ${id} des actualités sur le covoiturage de courte distance`,
  }
}

export async function generateStaticParams() {
  const query = {
    pagination: {
      pageSize: cmsActusByPage
    }
  };
  const { meta }  = await fetchAPI('/articles',query);
  const nbPage = meta.pagination.pageCount;
  return Array.from({ length: nbPage }, (_, v) => {
    const id = v + 1;
    return {
      id: id.toString(),
    }
  });
}

export default async function ActuPage({ params }: { params: Promise<{ id: string }>}) {
  const { id } = await params;
  const query = {
    populate: 'img',
    sort:'createdAt:desc',
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
  const pageTitle=`Actualités page ${id}`;
  const nbPage = meta.pagination.pageCount;

  return (
    <div id='content'>
      <PageTitle title={pageTitle} />
      {categories.data && 
        <div className={fr.cx('fr-grid-row','fr-mb-3w')}>
          <CategoryTags categories={categories.data} page={id}/>
        </div>
      }
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
        defaultPage={Number(id)}
        href={`/actualites`}
      />     
    </div>
  );
}
