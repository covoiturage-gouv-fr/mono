import PageTitle from '@/components/common/PageTitle';
import { fr } from '@codegouvfr/react-dsfr';
import { Alert } from "@codegouvfr/react-dsfr/Alert";
import { Suspense } from 'react';
import Dashboard from './Dashboard';

export default function IncentiveCampaignsPage() {
  const title = 'Recensement des campagnes d\'incitation financière au covoiturage';
  const subtitle = 'Une campagne d’incitation financière est une subvention versée par une collectivité locale compétente (AOM) pour la réalisation de trajets en covoiturage sur son territoire.';
    return (
      <div id='content'>
        <PageTitle title={title} />
        <Alert
          description="Les données des campagnes affichées ci-dessous nous sont communiquées par les opérateurs. N’hésitez pas à revenir vers l’opérateur en question pour en ajouter une ou modifier du contenu."
          severity="info"
          small
        />
        <h2 className={fr.cx('fr-h4')}>{subtitle}</h2>
        <Suspense>
          <Dashboard />
        </Suspense>
    </div>
    
  );
}