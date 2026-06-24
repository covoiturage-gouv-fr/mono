import type { Metadata, Viewport } from "next";
import { DsfrProvider } from "../dsfr-bootstrap";
import { DsfrHead, getHtmlAttributes } from "../dsfr-bootstrap/server-only-index";


export const metadata: Metadata = {
  title: "Attestation sur l'honneur de covoiturage",
  description:
    "Ce service public numérique génère une attestation sur l'honneur de covoiturage pour le Forfait Mobilités Durables.",
  keywords: ["attestation", "covoiturage", "fmd", "forfait mobilités durables"],
  manifest: "/favicons/site.webmanifest",
  icons: {
    icon: [
      { url: "/favicons/favicon-32x32.png", sizes: "32x32", type: "image/png" },
      { url: "/favicons/favicon-16x16.png", sizes: "16x16", type: "image/png" },
      { url: "/favicons/favicon.png", type: "image/png" },
    ],
    apple: [{ url: "/favicons/apple-touch-icon.png", sizes: "180x180", type: "image/png" }],
    other: [{ rel: "mask-icon", url: "/favicons/safari-pinned-tab.svg", color: "#21bf73" }],
  },
  other: {
    "msapplication-TileColor": "#603cba",
    "msapplication-config": "/favicons/browserconfig.xml",
  },
};

export const viewport: Viewport = {
  themeColor: "#ffffff",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  const lang = "fr";
  return (
    <html {...getHtmlAttributes({ lang })} >
      <head>
        <DsfrHead preloadFonts={[
          "Marianne-Regular",
          "Marianne-Medium",
          "Marianne-Bold"]}
          />
      </head>
      <body>
        <DsfrProvider lang={lang}>
          {children}
        </DsfrProvider>
      </body>
    </html>
  );
}
