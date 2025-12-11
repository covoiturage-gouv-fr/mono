import { Card } from "@codegouvfr/react-dsfr/Card";
import { fr } from "@codegouvfr/react-dsfr";
import { Tag } from "@codegouvfr/react-dsfr/Tag";
import { ButtonsGroup } from "@codegouvfr/react-dsfr/ButtonsGroup";
import { ActuCardProps } from "@/interfaces/actualites/componentsInterface";

export default function ActuCard(props: ActuCardProps) {
  return(
    <Card
      title={props.title}
      desc={props.content}
      start={
        <ul className={fr.cx('fr-tags-group')}>
          {props.categories &&
            props.categories.map((c, i) => {
              return (
                <li key={i}>
                  <Tag small>
                    {c.attributes ? c.attributes.label : ''}
                  </Tag>
                </li>
              )
            })
          }
        </ul>
      }
      detail={<><span className={fr.cx('fr-icon-arrow-right-line','fr-icon--sm','fr-mr-1v')} aria-hidden="true"></span><span>Publié le {props.date}</span></>}
      imageAlt={""}
      imageUrl={props.img}
      shadow={true}
      footer={
        <ButtonsGroup
          alignment='right'
          buttonsEquisized
          buttons={[
            {
              children: 'Lire l\'actualité',
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