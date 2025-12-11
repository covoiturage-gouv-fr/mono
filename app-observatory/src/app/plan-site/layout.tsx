import { AppFooter } from '@/components/layout/AppFooter';
import { AppHeader } from '@/components/layout/AppHeader';
import { Follow } from '@/components/layout/Follow';
import { ScrollToTop } from '@/components/layout/ScrollToTop';
import { ContextProvider } from '@/context/ContextProvider';
import { fr } from '@codegouvfr/react-dsfr';

export default function DefaultLayout({ children }: { children: React.ReactNode }) {
  return(
    <ContextProvider>
      <AppHeader />
        <main tabIndex={-1}>
          <div className={fr.cx('fr-container')}>
            {children}
            <ScrollToTop />
          </div>
          <Follow />
        </main>
      <AppFooter />
    </ContextProvider>
  )
}