import { AppFooter } from "@/components/layout/AppFooter";
import { AppHeader } from "@/components/layout/AppHeader";
import { Follow } from "@/components/layout/Follow";
import { MatomoAnalytics } from "@/components/layout/MatomoAnalytics";
import { ScrollToTop } from "@/components/layout/ScrollToTop";
import { Skiplinks } from "@/components/layout/Skiplinks";
import {
  DsfrProvider,
  StartDsfrOnHydration,
} from "@/components/layout/dsfr-bootstrap";
import {
  DsfrHead,
  getHtmlAttributes,
} from "@/components/layout/dsfr-bootstrap/server-only-index";
import { AuthProvider } from "@/providers/AuthProvider";
import "@/styles/global.scss";
import MuiDsfrThemeProvider from "@codegouvfr/react-dsfr/mui";
import { type Metadata } from "next";
import { Suspense } from "react";

export const metadata: Metadata = {
  title: "partenaire.covoiturage.gouv.fr",
  description: "Développer le covoiturage de courte distance",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  const lang = "fr";
  return (
    <html {...getHtmlAttributes({ lang })}>
      <head>
        <DsfrHead
          preloadFonts={[
            //"Marianne-Light",
            //"Marianne-Light_Italic",
            "Marianne-Regular",
            //"Marianne-Regular_Italic",
            "Marianne-Medium",
            //"Marianne-Medium_Italic",
            "Marianne-Bold",
            //"Marianne-Bold_Italic",
            //"Spectral-Regular",
            //"Spectral-ExtraBold"
          ]}
        />
      </head>
      <body>
        <DsfrProvider lang={lang}>
          <StartDsfrOnHydration />
          <MuiDsfrThemeProvider>
            <AuthProvider>
              <Skiplinks />
              <AppHeader />
              <main tabIndex={-1}>
                {children}
                <ScrollToTop />
                <Follow />
              </main>
              <AppFooter />
              <Suspense fallback={null}>
                <MatomoAnalytics />
              </Suspense>
            </AuthProvider>
          </MuiDsfrThemeProvider>
        </DsfrProvider>
      </body>
    </html>
  );
}
