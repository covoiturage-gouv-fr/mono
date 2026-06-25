import * as z from "zod";

/**
 * The 8 mandatory "Je certifie que…" declarations (checkbox_4..checkbox_11 in the
 * legacy app). All must be checked to submit. They are NOT drawn on the PDF — they
 * are a submit gate only. checkbox_10's label is dynamic (days/year), built in the
 * component.
 */
export const CERTIFICATIONS = [
  { id: "checkbox_4", label: "Je ne bénéficie pas d'un logement de fonction ;" },
  { id: "checkbox_5", label: "Je ne bénéficie pas d'un véhicule de fonction sur le lieu de travail ;" },
  {
    id: "checkbox_6",
    label:
      "Je ne bénéficie pas d'un transport collectif gratuit entre mon domicile et mon lieu de travail (uniquement pour la fonction publique hospitalière) ;",
  },
  { id: "checkbox_7", label: "Je ne suis pas transporté gratuitement par mon employeur ;" },
  {
    id: "checkbox_8",
    label:
      "Je ne bénéficie pas pour le même trajet d'une prise en charge au titre des frais de déplacements temporaires ;",
  },
  { id: "checkbox_9", label: "Je ne bénéficie pas des dispositions du décret n°83-588 du 1er juillet 1983 ;" },
  { id: "checkbox_10", label: "" }, // dynamic label, set in the component
  {
    id: "checkbox_11",
    label: "Je tiens à disposition de mon employeur tout justificatif utile d'utilisation effective du covoiturage.",
  },
] as const;

export const publicFormSchema = z
  .object({
    name: z
      .string()
      .min(1, { message: "Les prénom et nom sont requis" })
      .max(51, { message: "Les prénom et nom ne peuvent dépasser 51 caractères" }),
    ministry: z
      .string()
      .min(1, { message: "L'employeur est requis" })
      .max(120, { message: "L'employeur ne peut dépasser 120 caractères" }),
    rank: z
      .string()
      .min(1, { message: "Le poste est requis" })
      .max(51, { message: "Le poste ne peut dépasser 51 caractères" }),
    workshare: z
      .string()
      .min(1, { message: "La quotité est requise" })
      .max(3, { message: "La quotité ne peut dépasser 3 caractères" })
      .regex(/^[0-9]*$/, { message: "Ce champ n'accepte que les chiffres" }),
    year: z.number(),
    mobility: z.enum(["no", "in", "out"]),
    mobility_date: z
      .string()
      .regex(/^(\d{2}\/\d{2}\/\d{4})?$/, { message: "Format attendu : JJ/MM/AAAA" })
      .optional(),
    days: z
      .string()
      .min(1, { message: "Le nombre de jours est requis" })
      .regex(/^[0-9]{0,6}$/, { message: "Ce champ n'accepte que les chiffres" })
      .refine((v) => !v || Number(v) <= 365, { message: "Le nombre de jours ne peut dépasser 365" }),
    home_address: z
      .string()
      .min(1, { message: "L'adresse du domicile est requise" })
      .max(256, { message: "L'adresse ne peut dépasser 256 caractères" }),
    work_address: z
      .string()
      .min(1, { message: "Le lieu de travail est requis" })
      .max(256, { message: "Le lieu de travail ne peut dépasser 256 caractères" }),
    certifications: z.array(z.string()),
    location: z
      .string()
      .min(1, { message: "La commune de génération est requise" })
      .max(128, { message: "La commune de génération ne peut dépasser 128 caractères" }),
  })
  // mobility_date is required only when a mobility (arrival/departure) is declared
  .refine((d) => d.mobility === "no" || !!d.mobility_date?.trim(), {
    message: "La date est requise",
    path: ["mobility_date"],
  })
  // every certification must be checked
  .refine((d) => d.certifications.length === CERTIFICATIONS.length, {
    message: "Vous devez certifier l'ensemble des déclarations",
    path: ["certifications"],
  });

export type PublicFormValues = z.infer<typeof publicFormSchema>;
