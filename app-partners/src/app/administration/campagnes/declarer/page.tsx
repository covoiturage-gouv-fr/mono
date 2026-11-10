import { type Metadata } from "next";
import CampagneFormPage from "../CampagneFormPage";

export const metadata: Metadata = { title: "Déclarer une campagne" };

export default function Page() {
  return <CampagneFormPage id={null} />;
}
