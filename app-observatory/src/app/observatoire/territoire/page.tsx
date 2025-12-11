import PageTitle from '@/components/common/PageTitle';
import { Alert } from "@codegouvfr/react-dsfr/Alert";
import { Suspense } from 'react';
import Dashboard from './Dashboard';


export default function Page() {

  const title = 'Comprendre le covoiturage quotidien sur votre territoire';
    return (
      <div id='content'>
        <PageTitle title={title} />
        <Alert
          description={
            <ul>
              <li>sont les données issues des plates-formes de covoiturage courte distance uniquement, à savoir 4% du covoiturage global selon l’enquête Mobilité des personnes 2022</li>
              <li>ont été réactualisées après exclusion des trajets présentant des anomalies ou de la fraude manifeste. Un léger delta dans les données peut donc être constaté avec des analyses réalisées par le passé.</li>
            </ul>
          }
          severity="info"
          title="A noter que les données présentées sur l’observatoire:"
        />
        <Suspense>
          <Dashboard />
        </Suspense> 
    </div>
    
  );
}
