import { headerFooterDisplayItem } from "@codegouvfr/react-dsfr/Display";
import { Footer } from "@codegouvfr/react-dsfr/Footer";

export function VitrineFooter() {
  return (
    <Footer
      id="footer"
      linkList={[
        {
          categoryName: "Startup d'Etat Covoiturage.beta.gouv",
          links: [
            {
              linkProps: {
                href: "https://doc.covoiturage.beta.gouv.fr/bienvenue/qui-sommes-nous",
                target: "_blank",
                title: "Qui sommes-nous ? | nouvelle fenêtre",
                "aria-label": "Qui sommes-nous ? | nouvelle fenêtre",
              },
              text: "Qui sommes-nous ?",
            },
            {
              linkProps: {
                href: "https://doc.covoiturage.beta.gouv.fr",
                target: "_blank",
                title: "Documentation | nouvelle fenêtre",
                "aria-label": "Documentation | nouvelle fenêtre",
              },
              text: "Documentation",
            },
            {
              linkProps: {
                href: "https://covoiturage.beta.gouv.fr/statistiques",
                target: "_blank",
                title: "Notre impact | nouvelle fenêtre",
                "aria-label": "Notre impact | nouvelle fenêtre",
              },
              text: "Notre impact",
            },
          ],
        },
        {
          categoryName: "Autres liens utiles",
          links: [
            {
              linkProps: {
                href: "https://www.expertises-territoires.fr/",
                target: "_blank",
                title: "Expertise territoire | nouvelle fenêtre",
                "aria-label": "Expertise territoire | nouvelle fenêtre",
              },
              text: "Expertises-territoires",
            },
            {
              linkProps: {
                href: "https://www.ecologie.gouv.fr/",
                target: "_blank",
                title: "Ecologie.gouv | nouvelle fenêtre",
                "aria-label": "Ecologie.gouv | nouvelle fenêtre",
              },
              text: "Ecologie.gouv",
            },
            {
              linkProps: {
                href: "https://www.ademe.fr/",
                target: "_blank",
                title: "ADEME | nouvelle fenêtre",
                "aria-label": "ADEME | nouvelle fenêtre",
              },
              text: "ADEME",
            },
            {
              linkProps: {
                href: "https://www.cerema.fr/",
                target: "_blank",
                title: "Cerema | nouvelle fenêtre",
                "aria-label": "Cerema | nouvelle fenêtre",
              },
              text: "Cerema",
            },
          ],
        },
      ]}
      accessibility="partially compliant"
      termsLinkProps={{
        href: "https://observatoire.covoiturage.gouv.fr/mentions-legales",
        title: "mentions légales | Observatoire.covoiturage.gouv.fr",
        "aria-label": "mentions légales",
      }}
      accessibilityLinkProps={{
        href: "https://observatoire.covoiturage.gouv.fr/accessibilite",
        title: "Accessibilité | Observatoire.covoiturage.gouv.fr",
        "aria-label": "Accessibilité",
      }}
      contentDescription="Le site officiel d’information sur le covoiturage de courte distance.
     Retrouvez toutes les informations et démarches administratives nécessaires au développement du covoiturage sur votre territoire."
      operatorLogo={{
        alt: "Registre de Preuve de Covoiturage",
        imgUrl:
          "https://static.covoiturage.beta.gouv.fr/logo_rpc_d82e4b3a4a.png",
        orientation: "horizontal",
      }}
      partnersLogos={{
        sub: [
          {
            alt: "ADEME",
            href: "https://www.ademe.fr",
            imgUrl:
              "https://www.ademe.fr/wp-content/uploads/2022/11/ademe-logo-2022-1.svg",
          },
          {
            alt: "beta.gouv.fr",
            href: "https://beta.gouv.fr/",
            imgUrl:
              "https://static.covoiturage.beta.gouv.fr/Capture_d_ecran_2024_07_11_a_18_30_59_ccd74c153f.png",
          },
          {
            alt: "Ile de france mobilité",
            href: "https://www.iledefrance-mobilites.fr/",
            imgUrl:
              "https://static.covoiturage.beta.gouv.fr/Logo_Ile_de_france_Mobilite_1668794ffa.png",
          },
        ],
      }}
      bottomItems={[headerFooterDisplayItem]}
      license={
        <>
          Sauf mention contraire, tous les contenus de ce site sont sous{" "}
          <a
            href={"https://github.com/etalab/licence-ouverte/blob/master/LO.md"}
            target="_blank"
            title="licence etalab-2.0 | nouvelle fenêtre"
            aria-label="licence etalab-2.0"
          >
            licence etalab-2.0
          </a>{" "}
          Toutes les illustrations sont réalisées par
          <a
            href={"https://www.freepik.com"}
            target="_blank"
            title="Freepik | nouvelle fenêtre"
            aria-label="Freepik | nouvelle fenêtre"
          >
            Freepik
          </a>
        </>
      }
    />
  );
}
