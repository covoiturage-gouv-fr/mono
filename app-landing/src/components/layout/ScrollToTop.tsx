import { fr } from '@codegouvfr/react-dsfr';
export function ScrollToTop(){
  return(
    <div className={fr.cx('fr-grid-row','fr-mt-5w')}>
      <div className={fr.cx('fr-col','fr-col-12')}>
        <a className={fr.cx('fr-link', 'fr-icon-arrow-up-fill', 'fr-link--icon-left')} 
          href="#top"
          title="Haut de page"
          aria-label="Haut de page"
        >
          Haut de page
        </a>
      </div>
    </div>
  )
}