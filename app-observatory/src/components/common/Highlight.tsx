import { fr } from "@codegouvfr/react-dsfr";
import ButtonsGroup from "@codegouvfr/react-dsfr/ButtonsGroup";
import { Highlight } from "@codegouvfr/react-dsfr/Highlight";
import { HighlightProps }  from "@/interfaces/common/componentsInterface"
import Image from 'next/image';
import { ButtonProps } from "@codegouvfr/react-dsfr/Button";

export default function AppHighlight(props: HighlightProps) {
  return(
    <div className={fr.cx('fr-grid-row','fr-mb-5w')}>
      {
        props.img &&
        <div className={fr.cx('fr-col-md-2')}>
          <figure className={fr.cx('fr-content-media')} style={{"margin":"0"}} role="group">
            <div className={fr.cx('fr-content-media__img')}>
              <Image className={fr.cx('fr-responsive-img','fr-responsive-img--4x3')} src={props.img} alt={''} width={120} height={80} />
            </div>
          </figure>
        </div>
      }
      <div className={!props.img ? fr.cx('fr-col-12') : fr.cx('fr-col-md-10')}>
        <h3 className={fr.cx('fr-h3', 'fr-ml-md-5w')}>{props.title}</h3>
        <Highlight classes={props.classes}>
          {props.content}
        </Highlight>
        {
          props.buttons && 
          <ButtonsGroup
            alignment={'right'}
            inlineLayoutWhen={'always'}
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
            buttonsIconPosition={'right'}
          />
        }
      </div>
    </div>
  );
}