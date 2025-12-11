import Block from "@/components/common/Block";
import Hero from "@/components/common/Hero";
import Highlight from '@/components/common/Highlight';
import MDContent from "@/components/common/MDContent";
import SectionTitle from "@/components/common/SectionTitle";
import { AppFooter } from '@/components/layout/AppFooter';
import { AppHeader } from '@/components/layout/AppHeader';
import { Follow } from '@/components/layout/Follow';
import { ScrollToTop } from '@/components/layout/ScrollToTop';
import Rows from '@/components/observatoire/indicators/Rows';
import RessourceCard from "@/components/ressources/RessourceCard";
import { ContextProvider } from '@/context/ContextProvider';
import { fetchAPI, shorten } from "@/helpers/cms";
import { fr } from "@codegouvfr/react-dsfr";
import { Tile } from "@codegouvfr/react-dsfr/Tile";
import { Metadata } from 'next';


export const metadata: Metadata = {
  title: 'Accueil | Observatoire.covoiturage.gouv.fr',
  description: 'Développer le covoiturage de courte distance',
}

export default async function Home() {
  const query = {
    filters: {
      slug: {
        $eq: 'accueil',
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
        populate: '*'
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
  const data = response.data[0]
  const hero = data.attributes.hero
  const block = data.attributes.block
  const list = data.attributes.list
  const rows = data.attributes.rows
  const resources = data.attributes.resources

  const tiles = [{
    desc:'Forfait mobilités durables, animations covoiturage en entreprise, mise en relation entre salariés, charte employeur engagé dans le covoiturage',
    grey: true,
    imageUrl: "https://static.covoiturage.beta.gouv.fr/Entreprise_ffe13e5025.svg",
    linkProps:{
      href: '/autres-acteurs/employeurs',
      title:"Vous êtes une entreprise"
    },
    title:"Une entreprise"
  },{
    desc:"Un bonus de 100€ pour les conducteurs réalisant leurs premiers trajets en covoiturage, un forfait mobilités durable jusqu'à 800€ par ans versé par votre employeur, quelques euros versés par votre collectivité à chaque trajet.",
    grey: true,
    imageUrl: "https://static.covoiturage.beta.gouv.fr/avatar_907efa139c.svg",
    linkProps:{
      href: '/autres-acteurs/covoitureurs',
      title:"Vous êtes un particulier"
    },
    title:"Un particulier"
  },{
    desc:"Découvrir le registre de preuve de covoiturage et en devenir partenaire",
    grey: true,
    imageUrl: "https://static.covoiturage.beta.gouv.fr/application_ca50922463.svg",
    linkProps:{
      href: '/autres-acteurs/plateformes',
      title:"Vous êtes une plateforme de covoiturage"
    },
    title:"Une plateforme de covoiturage"
  }
]

  return (
    <ContextProvider>
      <AppHeader />
        <main tabIndex={-1}>
          <div className={fr.cx('fr-container')}>
          <div id='content'>
            { hero && <Hero 
                title={hero.title} 
                subtitle={hero.subtitle}
                content={hero.content} 
                img={hero.img.data ? hero.img.data.attributes.formats.medium.url : undefined} 
                alt={hero.alt} 
                buttons={hero.buttons} 
              />
            }
            {data.content && 
              <div className={fr.cx('fr-grid-row','fr-mt-5w')}>
                <div className={fr.cx('fr-col')}>
                  <div>
                    <MDContent source={data.content} />
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
            <SectionTitle title='Vous êtes ?' />
            <div className={fr.cx('fr-grid-row','fr-grid-row--gutters')}>
              {tiles && tiles.map( (t, i) => 
                <div key={i} className={fr.cx('fr-col-12','fr-col-md-4')}>
                  <Tile 
                    title={t.title}
                    desc={t.desc}
                    grey={t.grey}
                    imageUrl={t.imageUrl}
                    imageAlt={''}
                    linkProps={t.linkProps}
                  />
                </div>
              )}
            </div>
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
          </div>
            <ScrollToTop />
          </div>
          <Follow />
        </main>
      <AppFooter />
    </ContextProvider>
  );
}
