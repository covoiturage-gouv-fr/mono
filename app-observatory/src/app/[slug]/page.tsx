import Block from "@/components/common/Block";
import Hero from "@/components/common/Hero";
import Highlight from '@/components/common/Highlight';
import MDContent from "@/components/common/MDContent";
import PageTitle from "@/components/common/PageTitle";
import SectionTitle from "@/components/common/SectionTitle";
import Rows from '@/components/observatoire/indicators/Rows';
import RessourceCard from "@/components/ressources/RessourceCard";
import { fetchAPI, shorten } from "@/helpers/cms";
import { fr } from "@codegouvfr/react-dsfr";

export async function generateMetadata({ params }: { params: Promise<{ slug: string }>}) {
  const { slug } = await params;
  const query = {
    filters: {
      slug: {
        $eq: slug,
      },
      tags:{
        slug:{
          $eq: 'commun',
        }
      }
    },
    fields:['title','content']
  };
  const {data}  = await fetchAPI('/pages',query);
  return {
    title: `${data ? data[0].attributes.title : ''} | Observatoire.covoiturage.gouv.fr`,
    description: shorten(`${data ? data[0].attributes.content :
    'Développer le covoiturage courte distance'}`,150),
  }
}

export async function generateStaticParams() {
  const query = {
    filters: {
      tags:{
        slug:{
          $eq: 'commun',
        }
      }
    },
    fields:['slug'],
    pagination: {
      limit:-1,
    },
  };
  const response  = await fetchAPI('/pages',query);
  const data = response.data
  return data ? data.map((p:any) => ({
    slug: p.attributes.slug,
  })) : []
}

export default async function CommunSinglePage({ params }: { params: Promise<{ slug: string }>}) {
  const { slug } = await params;
  const query = {
    filters: {
      slug: {
        $eq: slug,
      },
    },
    populate: {
      hero: {
        populate: 'buttons,img'
      },
      block: {
        populate: 'buttons,img'
      },
      list: {
        populate: 'buttons,img'
      },
      rows:{
        populate: '*'
      },
      resources:{
        populate: '*'
      }
    },  
  };
  const response  = await fetchAPI('/pages',query);
  const data = response.data[0];
  const hero = data.attributes.hero;
  const block = data.attributes.block;
  const list = data.attributes.list;
  const rows = data.attributes.rows;
  const resources = data.attributes.resources;
    
  return(
    <div id='content'>
      {!hero && 
        <PageTitle title={data ? data.attributes.title : ''} />
      }
      {hero && 
        <Hero 
          title={hero.title} 
          subtitle={hero.subtitle}
          content={hero.content} 
          img={hero.img.data ? hero.img.data.attributes.url : undefined} 
          alt={hero.alt} 
          buttons={hero.buttons} 
        />
      }
      {data && data.attributes.content && 
        <div className={fr.cx('fr-grid-row','fr-mt-5w')}>
          <div className={fr.cx('fr-col')}>
            <div className={fr.cx('fr-text--lg')}>
              <MDContent source={data.attributes.content} />
            </div>
          </div>
        </div>
      }
      {block &&
        <Block 
          title={block.title} 
          content={block.content} 
          img={block.img.data.attributes.url} 
          alt={block.alt} 
          buttons={block.buttons} 
        />
      }
      {rows && 
        <Rows data={rows} />
      }
      {list && 
        <div className={fr.cx('fr-grid-row')}>
          {list.map((l:any, i:number) => 
            l.__component === 'page.highlight' 
            ? <Highlight key={i} 
                title={l.title}
                content={l.content}
                img={l.img.data.attributes.url}
                buttons={l.buttons}
                classes={l.classes} 
              /> 
            : <SectionTitle key={i} title={l.title} />        
          )}
        </div>
      }
      {resources.data.length > 0 && 
        <>
          <SectionTitle title='Ressources' />
          <div className={fr.cx('fr-grid-row','fr-grid-row--gutters')}>
            {resources.data.map((r:any, i:number) =>  
              <div key={i} className={fr.cx('fr-col', 'fr-col-md-4')}>
                <RessourceCard 
                  title={r.attributes.title}
                  content={shorten(r.attributes.content, 100)}
                  date={new Date(r.attributes.public_date).toLocaleDateString('fr-FR')}
                  href={`/ressources/${r.attributes.slug}`}
                  img={r.attributes.img.data ? r.attributes.img.data.attributes.url : null}           
                />
              </div>
            )}
          </div>
        </>
      }
      {data.attributes.complement &&
        <div className={fr.cx('fr-grid-row','fr-mt-5w')}>
          <SectionTitle title='Ressources complémentaires' />
          <div className={fr.cx('fr-grid-row')}>
            <div className={fr.cx('fr-col')}>
              <div className={fr.cx('fr-text--lg')}>
                <MDContent source={data.attributes.complement} />
              </div>
            </div>
          </div>
        </div>
      }
    </div>
  );
}