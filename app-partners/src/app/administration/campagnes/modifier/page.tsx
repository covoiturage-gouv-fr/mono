import { type Metadata } from "next";
import { Suspense } from "react";
import ModifierCampagne from "./ModifierCampagne";

export const metadata: Metadata = { title: "Modifier une campagne" };

export default function Page() {
  return (
    <Suspense fallback={<p>Chargement…</p>}>
      <ModifierCampagne />
    </Suspense>
  );
}
