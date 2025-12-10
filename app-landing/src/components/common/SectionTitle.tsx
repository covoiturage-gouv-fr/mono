import { fr } from '@codegouvfr/react-dsfr';

export default function SectionTitle({ title }: { title: string }) {
  return (
    <div className={fr.cx('fr-grid-row', 'fr-mt-5w')}>
      <div className={fr.cx('fr-col')}>
        <h2>{title}</h2>
      </div>
    </div>
  );
}
