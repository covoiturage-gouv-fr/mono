import { Header } from '@codegouvfr/react-dsfr/Header';
import Navigation from './Navigation';
import SearchBar from './SearchBar';

export function AppHeader() {
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
        href: '/',
        title: 'Accueil | Observatoire.covoiturage.gouv.fr',
        "aria-label": 'Accueil | Observatoire.covoiturage.gouv.fr',
      }}
      serviceTitle='Observatoire.covoiturage.gouv.fr'
      serviceTagline='Développer le covoiturage courte distance'
      navigation={<Navigation />}
      renderSearchInput={({ className, id, placeholder, type }) => (
        <SearchBar
            className={className}
            id={id}
            placeholder={placeholder}
            type={type}
        />
      )}
    />
  );
}
