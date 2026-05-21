import Anonymous from "@/components/common/Anonymous";
import PageTitle from "@/components/common/PageTitle";
import { Config } from "@/config";
import { fr } from "@codegouvfr/react-dsfr";
import { ButtonsGroup } from "@codegouvfr/react-dsfr/ButtonsGroup";
import ProConnectButton from "@codegouvfr/react-dsfr/ProConnectButton";
import { type Metadata } from "next";
import Image from "next/image";

export const metadata: Metadata = {
  title: "Espace partenaire du Registre de Preuve de Covoiturage",
  description: "Développer le covoiturage de courte distance",
};

export default function Home() {
  const proconnectUrl = Config.get<string>("auth.domain") + "/auth/login";
  return (
    <div className={fr.cx("fr-container")}>
      <Anonymous />
      <div
        id="content"
        className={fr.cx("fr-grid-row", "fr-grid-row--gutters", "fr-grid-row--center", "fr-grid-row--middle")}
      >
        <div className={fr.cx("fr-col-12", "fr-col-sm-6")} style={{ wordBreak: "break-word", minWidth: 0 }}>
          <PageTitle title={`Bienvenue sur l'espace partenaire de covoiturage.beta.gouv.fr`} />
        </div>
        <div className={fr.cx("fr-col-12", "fr-col-sm-6", "fr-hidden", "fr-unhidden-sm")}>
          <Image
            src="https://static.covoiturage.beta.gouv.fr/medium_Car_driving_bro_d766ec81d5.png"
            alt=""
            width={450}
            height={450}
          />
        </div>
      </div>
      <div>
        <div className={fr.cx("fr-mb-7w")}>
          <h5>Connectez-vous à votre compte :</h5>
          <ProConnectButton url={proconnectUrl} classes={{ root: "proconnect-no-help" }} />
        </div>
        <div>
          <h5>Vous souhaitez devenir partenaire ?</h5>
          <ButtonsGroup
            buttons={[
              {
                style: { width: "auto" },
                children: "Collectivité, découvrez les étapes pour créer un compte territoire et/ou utilisateur",
                linkProps: {
                  href: "https://doc.covoiturage.beta.gouv.fr/nos-services/le-registre-de-preuve-de-covoiturage/comment-fonctionne-lespace-partenaire/ouvrir-son-compte",
                  title: `Collectivité, découvrez les étapes pour créer un compte territoire et/ou utilisateur`,
                  "aria-label": `Collectivité, découvrez les étapes pour créer un compte territoire et/ou utilisateur`,
                },
                priority: "secondary",
                className: "no-external-icon",
              },
              {
                style: { width: "auto" },
                children: "Opérateurs, découvrez les étapes",
                linkProps: {
                  href: "https://doc.covoiturage.beta.gouv.fr/vous-etes/je-suis-un-operateur/se-connecter#id-3.1-se-creer-un-compte-proconnect",
                  title: `Devenir partenaire - nouvelle fenêtre`,
                  "aria-label": `Devenir partenaire  - nouvelle fenêtre`,
                  target: "_blank",
                  className: "no-external-icon",
                },
                priority: "secondary",
              },
            ]}
          />
        </div>
      </div>
      <div className={fr.cx("fr-mt-4w", "fr-hidden-sm")}>
        <Image
          src="https://static.covoiturage.beta.gouv.fr/medium_Car_driving_bro_d766ec81d5.png"
          alt=""
          width={450}
          height={450}
          style={{ width: "100%", height: "auto" }}
        />
      </div>
    </div>
  );
}
