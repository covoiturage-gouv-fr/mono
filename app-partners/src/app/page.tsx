import Anonymous from "@/components/common/Anonymous";
import PageTitle from "@/components/common/PageTitle";
import { fr } from "@codegouvfr/react-dsfr";
import { ButtonsGroup } from "@codegouvfr/react-dsfr/ButtonsGroup";
import { type Metadata } from "next";
import Image from "next/image";

export const metadata: Metadata = {
  title: "Espace partenaire du Registre de Preuve de Covoiturage",
  description: "Développer le covoiturage de courte distance",
};

export default function Home() {
  return (
    <div className={fr.cx("fr-container")}>
      <Anonymous />
      <div id="content" className="text-center">
        <PageTitle title={`Espace partenaire de covoiturage.beta.gouv.fr`} />
        <div>
          <p>Pour exporter les données des trajets</p>
          <p>Pour un suivi des campagnes d’incitation</p>
        </div>
        <div className={fr.cx("fr-grid-row", "fr-grid-row--gutters", "fr-grid-row--center")}>
          <div className={fr.cx("fr-col-12", "fr-col-md-6")}>
            <div className={fr.cx("fr-h2")}>Comment devenir partenaire ?</div>
            <ButtonsGroup
              buttons={[
                {
                  children: "Collectivité, découvrez les étapes pour créer un compte territoire et/ou utilisateur",
                  linkProps: {
                    href: "https://doc.covoiturage.beta.gouv.fr/nos-services/le-registre-de-preuve-de-covoiturage/comment-fonctionne-lespace-partenaire/ouvrir-son-compte",
                    title: `Collectivité, découvrez les étapes pour créer un compte territoire et/ou utilisateur`,
                    "aria-label": `Collectivité, découvrez les étapes pour créer un compte territoire et/ou utilisateur`,
                  },
                  priority: "primary",
                },
                {
                  children: "Opérateurs, découvrez les étapes",
                  linkProps: {
                    href: "https://doc.covoiturage.beta.gouv.fr/vous-etes/je-suis-un-operateur/se-connecter#id-3.1-se-creer-un-compte-proconnect",
                    title: `Devenir partenaire - nouvelle fenêtre`,
                    "aria-label": `Devenir partenaire  - nouvelle fenêtre`,
                    target: "_blank",
                  },
                  priority: "secondary",
                },
              ]}
            />
            <Image
              src="https://static.covoiturage.beta.gouv.fr/medium_Car_driving_bro_d766ec81d5.png"
              alt=""
              width={450}
              height={450}
            />
          </div>
        </div>
      </div>
    </div>
  );
}
