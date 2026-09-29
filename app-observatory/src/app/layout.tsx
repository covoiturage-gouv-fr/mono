import JDMA from "@/components/common/JDMA";
import { MatomoAnalytics } from "@/components/layout/MatomoAnalytics";
import { Skiplinks } from "@/components/layout/Skiplinks";
import {
  DsfrProvider,
  StartDsfrOnHydration,
} from "@/components/layout/dsfr-bootstrap";
import {
  DsfrHead,
  getHtmlAttributes,
} from "@/components/layout/dsfr-bootstrap/server-only-index";
import MuiDsfrThemeProvider from "@codegouvfr/react-dsfr/mui";
import { Metadata } from "next";
import { Suspense } from "react";
import "../styles/global.scss";

export const metadata: Metadata = {
  title:
    "Comprendre le covoiturage quotidien sur votre territoire | Observatoire.covoiturage.gouv.fr",
  description:
    "Tableau de bord pour comprendre le covoiturage de courte distance",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  //NOTE: The lang parameter is optional and defaults to "fr"
  const lang = "fr";
  return (
    <html {...getHtmlAttributes({ lang })}>
      <head>
        <Suspense fallback={null}>
          <MatomoAnalytics />
        </Suspense>
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
            <Skiplinks />
            {children}
          </MuiDsfrThemeProvider>
          <JDMA />
        </DsfrProvider>
      </body>
    </html>
  );
}
