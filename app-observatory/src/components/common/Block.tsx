import { ButtonsGroup } from '@codegouvfr/react-dsfr/ButtonsGroup';
import { fr } from '@codegouvfr/react-dsfr';
import { ButtonProps } from '@codegouvfr/react-dsfr/Button';
import Image from 'next/image';
import { BlockProps } from '@/interfaces/common/componentsInterface';

export default function Block(props:BlockProps) {
  return (
    <div className={fr.cx('fr-grid-row','fr-mt-5w')}>
      <div className={fr.cx('fr-col-12', 'fr-col-md-9')}>
        <h2 className={fr.cx('fr-h2','fr-mb-2v')}>{props.title}</h2>
        <p className={fr.cx('fr-pr-md-5v')}>{props.content}</p>
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
          <div className={fr.cx('fr-content-media')}>
            <div className={fr.cx('fr-content-media__img')}>
              <Image className={fr.cx('fr-responsive-img','fr-responsive-img--16x9')} src={props.img} alt={''} width={120} height={80} aria-hidden={true} />
            </div>
          </div>
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