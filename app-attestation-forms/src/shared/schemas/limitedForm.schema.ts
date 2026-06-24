import * as z from "zod";

export const limitedFormSchema = z.object({
  name: z
    .string()
    .min(1, { message: "Le prénom est requis" })
    .max(128, { message: "Le prénom ne peut dépasser 128 caractères" }),
  address: z
    .string()
    .min(1, { message: "L'adresse du domicile est requise" })
    .max(256, { message: "L'adresse ne peut dépasser 256 caractères" }),
  employer: z
    .string()
    .min(1, { message: "Le nom de l'employeur est requis" })
    .max(256, { message: "Le nom de l'employeur ne peut dépasser 256 caractères" }),
  workshare: z
    .string()
    .min(1, { message: "La quotité est requise" })
    .max(3, { message: "La quotité ne peut dépasser 3 caractères" })
    .regex(/^[0-9]*$/, { message: "Ce champ n'accepte que les chiffres" }),
  distance: z
    .string()
    .regex(/^[0-9]{0,6}$/, { message: "Ce champ n'accepte que les chiffres" })
    .refine((v) => !v || Number(v) <= 100000, {
      message: "La distance ne peut dépasser 100000 km",
    })
    .optional(),
  days: z
    .string()
    .regex(/^[0-9]{0,6}$/, { message: "Ce champ n'accepte que les chiffres" })
    .refine((v) => !v || Number(v) <= 365, {
      message: "Le nombre de jours ne peut dépasser 365",
    })
    .optional(),
  location: z
    .string()
    .min(1, { message: "La commune est requise" })
    .max(128, { message: "La commune ne peut dépasser 128 caractères" }),
});

export type LimitedFormValues = z.infer<typeof limitedFormSchema>;
