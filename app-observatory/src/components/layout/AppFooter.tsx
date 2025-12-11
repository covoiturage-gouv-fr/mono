import { headerFooterDisplayItem } from '@codegouvfr/react-dsfr/Display';
import { Footer } from '@codegouvfr/react-dsfr/Footer';

export function AppFooter() {
  return (
    <Footer
      id='footer'
      linkList={[
        {
          categoryName: 'Startup d\'Etat Covoiturage.gouv',
          links: [
            {
              linkProps: {
                href: 'https://doc.covoiturage.beta.gouv.fr/bienvenue/qui-sommes-nous',
                target:'_blank',
                title:'Qui sommes-nous ? | nouvelle fenêtre',
                "aria-label":'Qui sommes-nous ? | nouvelle fenêtre'
              },
              text: 'Qui sommes-nous ?'
            },
            {
              linkProps: {
                href: 'https://doc.covoiturage.beta.gouv.fr/nos-services/le-registre-de-preuve-de-covoiturage',
                target:'_blank',
                title:'Nos services | nouvelle fenêtre',
                "aria-label":'Nos services | nouvelle fenêtre'
              },
              text: 'Nos services'
            },
            {
              linkProps: {
                href: 'https://doc.covoiturage.beta.gouv.fr/bienvenue/faq-foire-aux-questions',
                target:'_blank',
                title:'Foire aux questions | nouvelle fenêtre',
                "aria-label":'Foire aux questions | nouvelle fenêtre'
              },
              text: 'FAQ'
            },
          ]
        },
        {
          categoryName: 'Autres liens utiles',
          links: [
            {
              linkProps: {
                href: 'https://www.expertises-territoires.fr/',
                target:'_blank',
                title:'Expertise territoire | nouvelle fenêtre',
                "aria-label":'Expertise territoire | nouvelle fenêtre'
              },
              text: 'Expertises-territoires'
            },
            {
              linkProps: {
                href: 'https://www.ecologie.gouv.fr/',
                target:'_blank',
                title:'Ecologie.gouv | nouvelle fenêtre',
                "aria-label":'Ecologie.gouv | nouvelle fenêtre'
              },
              text: 'Ecologie.gouv'
            },
            {
              linkProps: {
                href: 'https://www.ademe.fr/',
                target:'_blank',
                title:'ADEME | nouvelle fenêtre',
                "aria-label":'ADEME | nouvelle fenêtre'
              },
              text: 'ADEME'
            },
            {
              linkProps: {
                href: 'https://www.cerema.fr/',
                target:'_blank',
                title:'Cerema | nouvelle fenêtre',
                "aria-label":'Cerema | nouvelle fenêtre'
              },
              text: 'Cerema'
            },
          ]
        }
      ]}
      accessibility='partially compliant'
      termsLinkProps={{
        href: '/mentions-legales',
        title: 'mentions légales | Observatoire.covoiturage.gouv.fr',
        "aria-label": 'mentions légales'
      }}
      websiteMapLinkProps={{
        href: '/plan-site',
        title: 'Plan du site | Observatoire.covoiturage.gouv.fr'
      }}
      accessibilityLinkProps={{
        href: '/accessibilite',
        title: 'Accessibilité | Observatoire.covoiturage.gouv.fr',
        "aria-label": 'Accessibilité'
      }}
      contentDescription='Le site officiel d’information sur le covoiturage de courte distance.
     Retrouvez toutes les informations et démarches administratives nécessaires au développement du covoiturage sur votre territoire.'
      operatorLogo={{
        alt: 'Registre de Preuve de Covoiturage',
        imgUrl: 'https://static.covoiturage.beta.gouv.fr/logo_rpc_d82e4b3a4a.png',
        orientation: 'horizontal',
      }}
      partnersLogos={{
        sub: [
          {
            alt: 'CEREMA',
            href: 'https://www.cerema.fr/fr/actualites/covoiturage-dossier-du-cerema',
            imgUrl: 'https://www.cerema.fr/themes/custom/uas_base/images/LogoCerema_horizontal.svg',
          },
          {
            alt: 'ADEME',
            href: 'https://www.ademe.fr',
            imgUrl: 'https://www.ademe.fr/wp-content/uploads/2022/11/ademe-logo-2022-1.svg',
          },
        ],
      }}
      bottomItems={[headerFooterDisplayItem]}
      license={
        <>
          Sauf mention contraire, tous les contenus de ce site sont sous{' '}
          <a href={'https://github.com/etalab/licence-ouverte/blob/master/LO.md'} 
            target="_blank" 
            title="licence etalab-2.0 | nouvelle fenêtre"
            aria-label="licence etalab-2.0"
          >
            licence etalab-2.0
          </a> 
          {' '}Toutes les illustrations sont réalisés par 
          <a href={'https://www.freepik.com'}
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