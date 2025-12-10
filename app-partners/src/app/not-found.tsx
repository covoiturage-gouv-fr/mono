import Button from '@codegouvfr/react-dsfr/Button';
import ovoidSvg from '@codegouvfr/react-dsfr/dsfr/artwork/background/ovoid.svg';
import technicalErrorSvg from '@codegouvfr/react-dsfr/dsfr/artwork/pictograms/system/technical-error.svg';
export default function NotFound() {
  return (
    <main role="main" id="content">
        <div className="fr-container">
            <div className="fr-my-7w fr-mt-md-12w fr-mb-md-10w fr-grid-row fr-grid-row--gutters fr-grid-row--middle fr-grid-row--center">
                <div className="fr-py-0 fr-col-12 fr-col-md-6">
                    <h1>Page non trouvée</h1>
                    <p className="fr-text--sm fr-mb-3w">Erreur 404</p>
                    <p className="fr-text--lead fr-mb-3w">La page que vous cherchez est introuvable. Excusez-nous pour la gène occasionnée.</p>
                    <p className="fr-text--sm fr-mb-5w">
                        Si vous avez tapé l'adresse web dans le navigateur, vérifiez qu'elle est correcte. La page n’est peut-être plus disponible.
                        <br/>Dans ce cas, pour continuer votre visite vous pouvez consulter notre page d’accueil, ou effectuer une recherche avec notre moteur de recherche en haut de page.
                        <br/>Sinon contactez-nous pour que l’on puisse vous rediriger vers la bonne information.
                    </p>
                    <ul className="fr-btns-group-icons-right fr-btns-group--inline-md">
                        <li>
                            <a className="fr-btn" href="/">
                                Page d'accueil
                            </a>
                        </li>
                        <li>
                          <Button
                                priority="secondary"
                                iconId='fr-icon-mail-line'
                                iconPosition="right"
                                linkProps={{
                                    "aria-label": 'Nous contacter - mailto',
                                    href: "mailto:contact@covoiturage.beta.gouv.fr"
                                }}
                            >
                                Contactez-nous
                            </Button>
                        </li>
                    </ul>
                </div>
                <div className="fr-col-12 fr-col-md-3 fr-col-offset-md-1 fr-px-6w fr-px-md-0 fr-py-0">
                    <svg xmlns="http://www.w3.org/2000/svg" className="fr-responsive-img fr-artwork" aria-hidden="true" width="160" height="200" viewBox="0 0 160 200">
                        <use className="fr-artwork-motif" href={`${ovoidSvg.src}#artwork-motif`}></use>
                        <use className="fr-artwork-background" href={`${ovoidSvg.src}#artwork-background`}></use>
                        <g transform="translate(40, 60)">
                            <use className="fr-artwork-decorative" href={`${technicalErrorSvg.src}#artwork-decorative`}></use>
                            <use className="fr-artwork-minor" href={`${technicalErrorSvg.src}#artwork-minor`}></use>
                            <use className="fr-artwork-major" href={`${technicalErrorSvg.src}#artwork-major`}></use>
                        </g>
                    </svg>
                </div>
            </div>
        </div>
    </main>
  )
}