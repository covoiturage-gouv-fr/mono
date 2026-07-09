import { Card } from "@codegouvfr/react-dsfr/Card";
import { ButtonsGroup } from "@codegouvfr/react-dsfr/ButtonsGroup";
import { RessourceCardProps } from "@/interfaces/ressources/componentsInterface";
import { excerpt } from "@/helpers/text";

export default function RessourceCard(props: RessourceCardProps) {

  return(
    <Card
      title={props.title}
      desc={excerpt(props.content)}
      detail={`Publié le ${props.date}`}
      imageAlt={''}
      imageUrl={props.img}
      footer={
        <ButtonsGroup
          alignment='right'
          buttonsEquisized
          buttons={[
            {
              children: 'En savoir plus',
              iconId: "fr-icon-arrow-right-s-line",
              iconPosition: "right",
              linkProps: {
                href: props.href,
              },
            },
          ]}
        />
      }
      horizontal={props.horizontal}
    />
  )
}