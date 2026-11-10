"use client";
import Alert from "@codegouvfr/react-dsfr/Alert";
import { useSearchParams } from "next/navigation";
import CampagneFormPage from "../CampagneFormPage";

export default function ModifierCampagne() {
  const id = Number(useSearchParams().get("id"));
  if (!Number.isInteger(id) || id <= 0) return <Alert severity="error" small description="Campagne introuvable." />;
  return <CampagneFormPage id={id} />;
}
