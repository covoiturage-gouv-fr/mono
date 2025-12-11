import { fr } from '@codegouvfr/react-dsfr';

export default function PageTitle({ title }: { title: string }) {
  return (
    <div id='page-title' className={fr.cx('fr-grid-row', 'fr-mt-5v')}>
      <div className={fr.cx('fr-col')}>
        <h1>{title}</h1>
      </div>
    </div>
  );
}
