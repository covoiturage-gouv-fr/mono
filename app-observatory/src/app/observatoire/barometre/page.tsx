import PageTitle from '@/components/common/PageTitle';
import { fr } from '@codegouvfr/react-dsfr';
import { CallOut } from '@codegouvfr/react-dsfr/CallOut';
import { Suspense } from 'react';
import Dashboard from './Dashboard';

export default function BarometerPage() {
  return (
    <div id='content'>
      <PageTitle title='Baromètre du covoiturage : mesurer le « covoiturage informel »' />
      <p className={fr.cx('fr-text--lead')}>
        Visualiser le covoiturage aux points de captage déployés sur le réseau routier français.
      </p>
      <h2 className={fr.cx('fr-h4')}>Contexte et objectif du baromètre</h2>
      <CallOut>
        <span className={fr.cx('fr-mb-2w')} style={{ display: 'block' }}>
          L&apos;Observatoire national du covoiturage visualise les trajets captés par les plateformes de
          covoiturage. Ce covoiturage intermédié ne représente que 4 % des trajets covoiturés : connaître le
          covoiturage informel est un enjeu fort pour l&apos;action publique.
        </span>
        <span className={fr.cx('fr-mb-2w')} style={{ display: 'block' }}>
          Le baromètre, porté par le Ministère de la Transition Écologique et ses partenaires, publie des
          indicateurs issus de capteurs et d&apos;enquêtes réalisés sur le réseau routier.
        </span>
        <span style={{ display: 'block' }}>
          La couverture des points de captage ne permet pas encore d&apos;établir des statistiques représentatives
          d&apos;un territoire. Les indicateurs donnent une information locale, au point de mesure, utile pour des
          analyses qualitatives ou des projets d&apos;aménagement (parking, pôle d&apos;échange multimodal, voie
          réservée) ou de services.
        </span>
      </CallOut>
      <Suspense>
        <Dashboard />
      </Suspense>
    </div>
  );
}
