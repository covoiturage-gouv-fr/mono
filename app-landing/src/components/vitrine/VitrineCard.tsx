import { VitrineCardProps } from "@/interfaces/vitrine/componentsInterfaces";
import { ButtonProps } from '@codegouvfr/react-dsfr/Button';
import { ButtonsGroup } from "@codegouvfr/react-dsfr/ButtonsGroup";
import { Card } from "@codegouvfr/react-dsfr/Card";
import Tag from '@codegouvfr/react-dsfr/Tag';

export default function VitrineCard(props: VitrineCardProps) {
  return(
    <Card
      title={props.title}
      desc={props.content}
      imageAlt={""}
      imageUrl={props.img}
      start={
        props.tags &&
        <ul className="fr-tags-group">
          {props.tags.map((t,i) =>{
            return(<li key={i}><Tag small>{t}</Tag></li>)}
          )}
        </ul>
      }
      shadow={true}
      footer={ props.buttons &&
        <ButtonsGroup
          alignment='left'
          buttonsEquisized
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
      horizontal={props.horizontal}
    />
  )
}