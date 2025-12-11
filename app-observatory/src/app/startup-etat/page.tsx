import Hero from '@/components/common/Hero';
import VitrineRows from '@/components/vitrine/VitrineRows';
import { fetchAPI } from '@/helpers/cms';
import { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'Accueil | Covoiturage.gouv.fr',
  description: 'Accélerateur de covoiturage de courte distance',
}

export default async function Vitrine() {
  const query = {
    filters: {
      id: {
        $eq: 1,
      },
    },
    populate: {
      Hero: {
        populate: 'buttons,img'
      },
      Rows:{
        populate: '*'
      }
    },  
  };
  const response  = await fetchAPI('/page-vitrines',query);
  const data = response.data[0];
  const hero = data?.attributes?.Hero;
  const rows = data?.attributes?.Rows;

  
  return(
    <div id='content'>
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
      {rows && 
        <VitrineRows data={rows} />
      }
    </div>
  )
}