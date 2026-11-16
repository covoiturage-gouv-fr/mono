import { roles } from "@/interfaces/auth";
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
    login_siren: z
      .string()
      .regex(/^\d{9}$/, { message: "Le SIREN doit contenir 9 chiffres" })
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
    if (row.role.startsWith("operator.") && !row.operator_id) {
      ctx.addIssue({ code: "custom", path: ["operator_id"], message: "Sélectionnez un opérateur" });
    }
    if (row.role.startsWith("territory.") && row.scopes?.length === 0) {
      ctx.addIssue({ code: "custom", path: ["scopes"], message: "Ajoutez au moins un périmètre" });
    }
  });
