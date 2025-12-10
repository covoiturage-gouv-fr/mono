import { ButtonsGroup } from '@codegouvfr/react-dsfr/ButtonsGroup';
import { fr } from '@codegouvfr/react-dsfr';
import { ButtonProps } from '@codegouvfr/react-dsfr/Button';
import Image from 'next/image';
import { HeroProps } from '@/interfaces/common/componentsInterface';
import MDContent from './MDContent';

export default function Hero(props:HeroProps) {
  return (
    <div id='hero' 
      className={fr.cx('fr-grid-row', 'fr-p-2w', 'fr-p-md-10w','fr-mt-5w')}
      style={{
        backgroundColor: fr.colors.decisions.background.contrast.blueFrance.default
      }}
    >
      <div className={fr.cx('fr-col-12', 'fr-col-md-9')}>
        <h1 className={fr.cx('fr-h1','fr-mb-2v')}>{props.title}</h1>
        {props.subtitle && <p className={fr.cx('fr-h6')}>{props.subtitle}</p>}
        <div className={fr.cx('fr-pr-md-5v')}>
          <MDContent source={props.content} /> 
        </div>
        {props.buttons && props.img && 
          <div className={fr.cx('fr-pr-md-5v')}>
            <ButtonsGroup
              alignment={'right'}
              buttons={props.buttons.map(b => {
                return {
                  children:b.title,
                  linkProps: b.url.startsWith('http') ? {
                    href: b.url,
                    title:`${b.title} - nouvelle fenêtre` ,
                    "aria-label":`${b.title} - nouvelle fenêtre`,
                    target:'_blank'
                  } : {
                    href: b.url,
                  },
                  iconId: b.icon ? b.icon : '',
                  priority: b.color ? b.color : 'primary',
                } 
              }) as [ButtonProps, ...ButtonProps[]]}
              buttonsEquisized
              buttonsIconPosition={'right'}
            />
          </div>
        }
      </div>
      <div className={fr.cx('fr-col', 'fr-col-md-3','fr-my-auto')}>
        {props.img &&
          <figure className={fr.cx('fr-content-media')} role="group">
            <div className={fr.cx('fr-content-media__img')}>
              <Image className={fr.cx('fr-responsive-img')} src={props.img} alt={''} width={120} height={80} />
            </div>
          </figure>
        }
        {props.buttons && !props.img && 
          <ButtonsGroup
            alignment={'center'}
            buttons={props.buttons.map(b => {
              return {
                children:b.title,
                linkProps: b.url.startsWith('http') ? {
                  href: b.url,
                  title:`${b.title} - nouvelle fenêtre` ,
                  "aria-label":`${b.title} - nouvelle fenêtre`,
                  target:'_blank'
                } : {
                  href: b.url,
                },
                iconId: b.icon ? b.icon : '',
                priority: b.color ? b.color : 'primary',
              } 
            }) as [ButtonProps, ...ButtonProps[]]}
          />
        }
      </div>
    </div>
  );
}