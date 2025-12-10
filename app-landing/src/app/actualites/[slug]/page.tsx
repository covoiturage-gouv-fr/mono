import MDContent from "@/components/common/MDContent";
import PageTitle from "@/components/common/PageTitle";
import Share from "@/components/common/Share";
import { Config } from "@/config";
import { fetchAPI, shorten } from "@/helpers/cms";
import { fr } from "@codegouvfr/react-dsfr";
import { ButtonProps } from '@codegouvfr/react-dsfr/Button';
import ButtonsGroup from '@codegouvfr/react-dsfr/ButtonsGroup';
import Tag from "@codegouvfr/react-dsfr/Tag";
import Image from 'next/image';
import { Button } from '../../../interfaces/cms/collectionsInterface';

export async function generateMetadata({ params }: { params: Promise<{ slug: string }>}) {
  const { slug } = await params;
  const query = {
    filters: {
      slug: {
        $eq: slug,
      }
    },
    fields:['title','description']
  };
  const response  = await fetchAPI('/articles',query);
  const data = response.data[0];
  return {
    title: `${data ? data.attributes.title : ''} | Observatoire.covoiturage.gouv.fr`,
    description: shorten(`${data && data.attributes.description ? data.attributes.description : ''}`,150),
  }
}

export async function generateStaticParams() {
  const query = {
    fields:['slug'],
    pagination: {
      limit:-1,
    },
  };
  const response  = await fetchAPI('/articles',query);
  const data = response.data
  return data ? data.map((p:any) => ({
    slug: p.attributes.slug,
  })) : []
}

export default async function ActuSingle({ params }: { params: Promise<{ slug: string }>}) {
  const { slug } = await params;
  const hostUrl = Config.get<string>('next.public_url', 'http://localhost:4200');
  const location = `${hostUrl}/actualites/${slug}`;
  const query = {
    filters: {
      slug: {
        $eq: slug,
      },
    },
    populate: '*',  
  };
  const response  = await fetchAPI('/articles',query);
  const data = response.data[0];
    
  const share = [
    {
      name:'Facebook', 
      icon:'fr-share__link--facebook', 
      href:`https://www.facebook.com/sharer.php?u=${location}`,
    },
    {
      name:'Twitter', 
      icon:'fr-share__link--twitter', 
      href:`https://twitter.com/intent/tweet?url=${location}`,
    },
    {
      name:'LinkedIn', 
      icon:'fr-share__link--linkedin', 
      href:`https://www.linkedin.com/shareArticle?url=${location}`,
    },
    {
      name:'Email', 
      icon:'fr-share__link--mail', 
      href:`mailto:?subject=${data ? data.attributes.title : ''}&body=${data ? data.attributes.description : ''} ${location}`,
    }
  ]

  return (
    <article id='actu-content'>
      <div className={fr.cx('fr-grid-row', 'fr-grid-row--gutters')}>
      { data && 
        <div className={fr.cx('fr-col','fr-col-12')}>
          <PageTitle title={data.attributes.title} />
          { data.attributes.createdAt &&
            <p>Publié le {new Date(data.attributes.createdAt).toLocaleDateString('fr-FR')}</p>
          }
          <div className={fr.cx('fr-grid-row', 'fr-grid-row--gutters')}>
            <div className={fr.cx('fr-col','fr-col-md-9')}>
              <ul className={fr.cx('fr-tags-group')}>
                {data.attributes.categories &&
                  data.attributes.categories.data.map((c:any, i:number) => {
                    return (
                      <li key={i}>
                        <Tag
                          linkProps={{
                            href: `/actualites/categorie/${c.attributes.slug}`
                          }}
                        >
                          {c.attributes.label}
                        </Tag>
                      </li>
                    )
                  })
                }
              </ul>
            </div>
            <div className={fr.cx('fr-col','fr-col-md-3')}>
              <Share social={share} location={location} />
            </div>
          </div>          
          <figure style={{textAlign:'center'}}>
              <Image className={fr.cx('fr-responsive-img', 'fr-responsive-img--16x9')} 
                src={data.attributes.img.data.attributes.formats.large ? data.attributes.img.data.attributes.formats.large.url: data.attributes.img.data.attributes.url } 
                alt={data.attributes.legend ? data.attributes.legend : ''} 
                width={data.attributes.img.data.attributes.formats.large ? data.attributes.img.data.attributes.formats.large.width : data.attributes.img.data.attributes.width} 
                height={data.attributes.img.data.attributes.formats.large ? data.attributes.img.data.attributes.formats.large.height : data.attributes.img.data.attributes.height} 
              />
            <figcaption className={fr.cx('fr-content-media__caption')}>{data.attributes.legend}</figcaption>
          </figure>
          <div>
            {data.attributes.description}
            <MDContent source={data.attributes.content} />
          </div>
        </div>
      }
      {
        data.attributes.buttons && 
        <ButtonsGroup
          alignment={'right'}
          inlineLayoutWhen={'always'}
          buttons={data.attributes.buttons.map((b:Button) => {
            return {
              children:b.title,
              linkProps: b.url.startsWith('http') ? {
                href: b.url,
                title:`${b.title} - nouvelle fenêtre` ,
                "aria-label":`${b.title} - nouvelle fenêtre`,
                target:'_blank',
                rel: "noopener noreferrer"
              } : {
                href: b.url,
              },
              iconId: b.icon ?? '',
              priority: b.color ?? 'primary',
            } 
          }) as [ButtonProps, ...ButtonProps[]]}
          buttonsIconPosition={'right'}
        />
      }
      </div>
    </article>
  );
}