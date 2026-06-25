import { notFound } from "next/navigation";

/**
 * No landing page — this is an embed-only widget. Visiting "/" directly is a 404;
 * the parent app embeds the specific form routes (salarie-secteur-prive /
 * salarie-secteur-public) via iframe.
 */
export default function RootNotFound() {
  notFound();
}
