import { Follow } from '@/components/layout/Follow';
import { ScrollToTop } from '@/components/layout/ScrollToTop';
import { VitrineFooter } from '@/components/vitrine/VitrineFooter';
import { VitrineHeader } from '@/components/vitrine/VitrineHeader';
import { VitrineTracking } from '@/components/vitrine/VitrineTracking';
import { fr } from '@codegouvfr/react-dsfr';

export default function VitrineLayout({ children }: { children: React.ReactNode }) {
  return (
    <>
      <VitrineTracking />
      <VitrineHeader />
        <main tabIndex={-1}>
          <div className={fr.cx('fr-container')}>
            {children}
            <ScrollToTop />
          </div>
          <Follow />
        </main>
      <VitrineFooter />
    </>
  );
}