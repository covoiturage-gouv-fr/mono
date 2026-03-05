import { Header } from "@codegouvfr/react-dsfr/Header";

export function VitrineHeader() {
  return (
    <Header
      brandTop={
        <>
          Ministère
          <br />
          de la transition
          <br />
          écologique
          <br />
          et de la cohésion
          <br />
          des territoires
        </>
      }
      homeLinkProps={{
        href: "/",
        title: "Accueil | Covoiturage.beta.gouv.fr",
        "aria-label": "Accueil | Covoiturage.beta.gouv.fr",
      }}
      serviceTitle="Covoiturage.beta.gouv.fr"
      serviceTagline="Accélérateur de covoiturage courte distance"
      quickAccessItems={[
        {
          iconId: "fr-icon-team-line",
          linkProps: {
            href: "https://partenaire.covoiturage.beta.gouv.fr",
            target: "_blank",
          },
          text: "Espace partenaire",
        },
        {
          iconId: "fr-icon-article-line",
          linkProps: {
            href: "https://doc.covoiturage.beta.gouv.fr",
            target: "_blank",
          },
          text: "Notre documentation",
        },
        {
          iconId: "fr-icon-car-line",
          linkProps: {
            href: "https://covoiturage.beta.gouv.fr/statistiques",
            target: "_blank",
          },
          text: "Notre impact",
        },
      ]}
    />
  );
}
