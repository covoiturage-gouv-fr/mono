import { ReactNode } from "react";

/**
 * Renders a field label with the required marker (red asterisk).
 * The asterisk is decorative (aria-hidden) — the semantic "required" meaning is
 * carried by the input's `required` / `aria-required` attribute, per RGAA.
 * Pair with a single note above the form: "Les champs marqués d'un * sont obligatoires."
 */
export function RequiredLabel({ children }: { children: ReactNode }) {
  return (
    <>
      {children}{" "}
      <span aria-hidden="true" style={{ color: "var(--text-default-error)" }}>
        *
      </span>
    </>
  );
}
