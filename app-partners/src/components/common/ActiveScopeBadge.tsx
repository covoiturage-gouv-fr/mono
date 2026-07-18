"use client";
import { activeScopeLabel } from "@/helpers/auth";
import { useAuth } from "@/providers/AuthProvider";
import Badge from "@codegouvfr/react-dsfr/Badge";

// Rappel du périmètre actif au point d'action (export, campagnes, confirmation).
export function ActiveScopeBadge() {
  const { user } = useAuth();
  const label = activeScopeLabel(user);
  if (!label) return null;
  return (
    <Badge as="span" severity="info" noIcon>
      Périmètre actif : {label}
    </Badge>
  );
}
