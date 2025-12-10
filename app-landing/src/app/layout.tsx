import Analytics from '@/components/layout/Analytics';
import { Skiplinks } from '@/components/layout/Skiplinks';
import { StartDsfr } from '@/components/layout/StartDsfr';
import { defaultColorScheme } from '@/components/layout/defaultColorScheme';
import MuiDsfrThemeProvider from '@codegouvfr/react-dsfr/mui';
import { DsfrHead } from '@codegouvfr/react-dsfr/next-appdir/DsfrHead';
import { DsfrProvider } from '@codegouvfr/react-dsfr/next-appdir/DsfrProvider';
import { getHtmlAttributes } from '@codegouvfr/react-dsfr/next-appdir/getHtmlAttributes';
import { Metadata } from 'next';
import Link from 'next/link';
import '../styles/global.scss';

export const metadata: Metadata = {
  title: 'Comprendre le covoiturage quotidien sur votre territoire | Observatoire.covoiturage.gouv.fr',
  description: 'Tableau de bord pour comprendre le covoiturage de courte distance',
}


export default function RootLayout({ children }: { children: React.ReactNode }) {
  //NOTE: The lang parameter is optional and defaults to "fr"
  const lang = 'fr';
  return (
    <html {...getHtmlAttributes({ defaultColorScheme, lang })}>
      <head>
        <Analytics />
        <StartDsfr />
        <DsfrHead
          Link={Link}
          preloadFonts={[
            //"Marianne-Light",
            //"Marianne-Light_Italic",
            'Marianne-Regular',
            //"Marianne-Regular_Italic",
            'Marianne-Medium',
            //"Marianne-Medium_Italic",
            'Marianne-Bold',
            //"Marianne-Bold_Italic",
            //"Spectral-Regular",
            //"Spectral-ExtraBold"
          ]}
        />
      </head>
      <body>
        <DsfrProvider>
          <MuiDsfrThemeProvider>
            <Skiplinks />
            {children}
          </MuiDsfrThemeProvider>
        </DsfrProvider>
      </body>
    </html>
  );
}
