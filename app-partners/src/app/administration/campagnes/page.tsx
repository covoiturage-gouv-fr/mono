import { type Metadata } from "next";
import { Suspense } from "react";
import CampagnesDeclareesPage from "./CampagnesDeclareesPage";

export const metadata: Metadata = { title: "Campagnes déclarées" };

export default function Page() {
  return (
    <Suspense fallback={<p>Chargement…</p>}>
      <CampagnesDeclareesPage />
    </Suspense>
  );
}
