import { roles } from "@/interfaces/auth";
import type { OperatorsInterface, UserScopeInput } from "@/interfaces/dataInterface";
import { z } from "zod";

// Bornes alignées sur l'API (Varchar 256) ; rognage pour refuser les blancs et normaliser l'email.
const name = (label: string) =>
  z
    .string()
    .trim()
    .min(3, { message: `${label} doit contenir au moins 3 caractères` })
    .max(256, { message: `${label} ne peut pas dépasser 256 caractères` })
    .regex(/^[^<>]*$/, { message: "Les caractères < et > ne sont pas autorisés" });

export const userFormSchema = z
  .object({
    firstname: name("Le prénom"),
    lastname: name("Le nom"),
    email: z
      .string()
      .trim()
      .toLowerCase()
      .email({ message: `L'adresse mail n'est pas valide` })
      .max(256, { message: "L'adresse mail ne peut pas dépasser 256 caractères" }),
    operator_id: z.number({ message: "Sélectionnez un opérateur" }).nullish(),
    territory_id: z.number({ message: "Sélectionnez un territoire" }).nullish(),
    role: z.enum(roles, { message: "Le rôle n'est pas valide" }),
    login_siret: z
      .string()
      .regex(/^\d{14}$/, { message: "Le SIRET doit contenir 14 chiffres" })
      .nullish(),
    scopes: z
      .array(
        z.object({
          territory_id: z.number().optional(),
          operator_id: z.number().optional(),
          is_default: z.boolean().optional(),
        }),
      )
      .optional(),
  })
  // Un rôle opérateur sans opérateur, ou territoire sans périmètre, est un compte sans accès.
  .superRefine((row, ctx) => {
    // Obligatoire hors registre. `undefined` = champ masqué (réservé registry.admin) : l'API retombe alors
    // sur le SIRET de l'opérateur ou du territoire.
    const needsSiret = row.role.startsWith("operator.") || row.role.startsWith("territory.");
    if (needsSiret && row.login_siret === null) {
      ctx.addIssue({
        code: "custom",
        path: ["login_siret"],
        message: "Le SIRET de connexion ProConnect est obligatoire (14 chiffres)",
      });
    }
    if (row.role.startsWith("operator.") && !row.operator_id) {
      ctx.addIssue({ code: "custom", path: ["operator_id"], message: "Sélectionnez un opérateur" });
    }
    if (row.role.startsWith("territory.") && row.scopes?.length === 0) {
      ctx.addIssue({ code: "custom", path: ["scopes"], message: "Ajoutez au moins un périmètre" });
    }
  });

// SIRET du périmètre par défaut (à défaut le premier). null, jamais "" : l'API n'accepte que null ou 14 chiffres.
export const suggestSiret = (scopes: UserScopeInput[]): string | null =>
  (scopes.find((s) => s.is_default) ?? scopes[0])?.siret ?? null;

// Une valeur saisie ou déjà enregistrée n'est jamais écrasée par le changement de périmètre.
export const siretAfterScopesChange = (current: string | null | undefined, scopes: UserScopeInput[]): string | null =>
  // eslint-disable-next-line @typescript-eslint/prefer-nullish-coalescing -- "" doit aussi être remplacé
  current || suggestSiret(scopes);

export const operatorSiret = (operators: OperatorsInterface["data"], operatorId: number | null): string | null =>
  // eslint-disable-next-line @typescript-eslint/prefer-nullish-coalescing -- siret "" = absent
  operators.find((o) => o.id === operatorId)?.siret || null;

// Le SIRET dépend de la famille du rôle (opérateur, territoire, registre) : il ne survit pas au changement de famille.
export const siretAfterRoleChange = (
  from: string,
  to: string,
  current: string | null | undefined,
  scopes: UserScopeInput[],
): string | null => {
  const family = (role: string) => role.split(".")[0];
  if (family(from) === family(to)) return current ?? null;
  return family(to) === "territory" ? suggestSiret(scopes) : null;
};
