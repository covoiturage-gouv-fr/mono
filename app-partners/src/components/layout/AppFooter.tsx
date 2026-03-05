import { headerFooterDisplayItem } from "@codegouvfr/react-dsfr/Display";
import { Footer } from "@codegouvfr/react-dsfr/Footer";

export function AppFooter() {
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
                href: "https://doc.covoiturage.beta.gouv.fr/nos-services/le-registre-de-preuve-de-covoiturage",
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
          categoryName: "Observatoire national du covoiturage",
          links: [
            {
              linkProps: {
                href: "https://observatoire.covoiturage.gouv.fr/observatoire/comprendre-covoiturage-quotidien/",
                target: "_blank",
                title: "Les statistiques nationales | nouvelle fenêtre",
                "aria-label": "Les statistiques nationales | nouvelle fenêtre",
              },
              text: "Les statistiques nationales",
            },
            {
              linkProps: {
                href: "https://observatoire.covoiturage.gouv.fr/observatoire/territoire/",
                target: "_blank",
                title: "Les statistiques sur votre territoire | nouvelle fenêtre",
                "aria-label": "Les statistiques sur votre territoire | nouvelle fenêtre",
              },
              text: "Les statistiques sur votre territoire",
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
      contentDescription="L’application web des collectivités et opérateurs de covoiturage courte distance  partenaires du Registre de Preuve de Covoiturage. Ces comptes privés donnent accès à une fonctionnalité d’export et à des rapports sur les campagnes d’incitations financières."
      operatorLogo={{
        alt: "Registre de Preuve de Covoiturage",
        imgUrl: "https://static.covoiturage.beta.gouv.fr/logo_rpc_d82e4b3a4a.png",
        orientation: "horizontal",
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
          Toutes les illustrations sont réalisés par
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
