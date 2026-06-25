"use client";

import { AddressAutocomplete } from "@/components/AddressAutocomplete";
import { RequiredLabel } from "@/components/RequiredLabel";
import { saveHonor } from "@/services/counter.service";
import { generatePublicPdf } from "@/services/pdfPublic.service";
import { CERTIFICATIONS, PublicFormValues, publicFormSchema } from "@/shared/schemas/publicForm.schema";
import { Button } from "@codegouvfr/react-dsfr/Button";
import { Checkbox } from "@codegouvfr/react-dsfr/Checkbox";
import { Input } from "@codegouvfr/react-dsfr/Input";
import { RadioButtons } from "@codegouvfr/react-dsfr/RadioButtons";
import { zodResolver } from "@hookform/resolvers/zod";
import { useEffect, useState } from "react";
import { Controller, useForm } from "react-hook-form";

const STORAGE_KEY = "formPublic";

export function PublicForm() {
  const currentYear = new Date().getFullYear();
  const previousYear = currentYear - 1;

  const [submitError, setSubmitError] = useState(false);
  const {
    register,
    handleSubmit,
    reset,
    watch,
    control,
    formState: { errors, isValid },
  } = useForm<PublicFormValues>({
    resolver: zodResolver(publicFormSchema),
    mode: "onChange",
    defaultValues: {
      name: "",
      ministry: "",
      rank: "",
      workshare: "",
      year: currentYear,
      mobility: "no",
      mobility_date: "",
      days: "",
      home_address: "",
      work_address: "",
      certifications: [],
      location: "",
    },
  });

  // reload saved data (client-only)
  useEffect(() => {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      if (saved) reset(JSON.parse(saved));
    } catch {
      localStorage.removeItem(STORAGE_KEY);
    }
  }, [reset]);

  // debounced auto-save
  useEffect(() => {
    let timeout: ReturnType<typeof setTimeout>;
    const subscription = watch((value) => {
      clearTimeout(timeout);
      timeout = setTimeout(() => {
        localStorage.setItem(STORAGE_KEY, JSON.stringify(value));
      }, 250);
    });
    return () => {
      clearTimeout(timeout);
      subscription.unsubscribe();
    };
  }, [watch]);

  const mobility = watch("mobility");
  const days = watch("days");
  const year = watch("year");

  const onSubmit = async (data: PublicFormValues) => {
    try {
      setSubmitError(false);
      await generatePublicPdf(data);
      saveHonor("public", "");
    } catch (err) {
      console.error(err);
      setSubmitError(true);
    }
  };

  const onReset = () => {
    reset();
    setSubmitError(false);
    localStorage.removeItem(STORAGE_KEY);
  };

  const certificationLabel = (id: string, fallback: string) =>
    id === "checkbox_10"
      ? `J'utilise le covoiturage pendant au moins le nombre de jours requis en fonction de ma situation pour l'année soit ${days || "..."} jours pour l'année ${year || "..."} ;`
      : fallback;

  return (
    <form onSubmit={handleSubmit(onSubmit)} onReset={onReset}>
      <h4>Secteur public</h4>

      <p>
        L'indemnité est un montant forfaitaire pouvant aller jusqu'à 300&nbsp;€ par an (exonérée d'impôts et de
        prélèvements sociaux) en fonction du nombre de déplacements réalisés :
      </p>
      <ul>
        <li>100 € pour un nombre de déplacements entre 30 et 59 jours ;</li>
        <li>200 € pour un nombre de déplacements entre 60 et 99 jours ;</li>
        <li>300 € pour un nombre de déplacements d'au moins 100 jours</li>
      </ul>

      <p className="fr-highlight fr-highlight--blue-ecume">

        <em>
          Les champs marqués d'un{" "}
          <span aria-hidden="true" style={{ color: "var(--text-default-error)" }}>
            *
          </span>{" "}
          sont obligatoires.

        </em>
      </p>

      <Input
        label={<RequiredLabel>Je soussigné·e</RequiredLabel>}
        nativeInputProps={{ ...register("name"), required: true }}
        state={errors.name ? "error" : "default"}
        stateRelatedMessage={errors.name?.message}
      />

      <Input
        label={<RequiredLabel>Employeur (Ministère / collectivité / établissement)</RequiredLabel>}
        nativeInputProps={{ ...register("ministry"), required: true }}
        state={errors.ministry ? "error" : "default"}
        stateRelatedMessage={errors.ministry?.message}
      />

      <Input
        label={<RequiredLabel>Poste</RequiredLabel>}
        nativeInputProps={{ ...register("rank"), required: true }}
        state={errors.rank ? "error" : "default"}
        stateRelatedMessage={errors.rank?.message}
      />

      <Input
        label={<RequiredLabel>Quotité de temps de travail (en pourcentage. 100 = temps plein)</RequiredLabel>}
        nativeInputProps={{ ...register("workshare"), required: true }}
        state={errors.workshare ? "error" : "default"}
        stateRelatedMessage={errors.workshare?.message}
      />

      <Controller
        control={control}
        name="year"
        render={({ field }) => (
          <RadioButtons
            legend="Année d'application"
            orientation="horizontal"
            options={[previousYear, currentYear].map((y) => ({
              label: String(y),
              nativeInputProps: { checked: field.value === y, onChange: () => field.onChange(y) },
            }))}
          />
        )}
      />

      <Controller
        control={control}
        name="mobility"
        render={({ field }) => (
          <RadioButtons
            legend={`Départ ou arrivée en ${year}`}
            orientation="horizontal"
            options={[
              { label: "Non", nativeInputProps: { checked: field.value === "no", onChange: () => field.onChange("no") } },
              { label: "Arrivée", nativeInputProps: { checked: field.value === "in", onChange: () => field.onChange("in") } },
              { label: "Départ", nativeInputProps: { checked: field.value === "out", onChange: () => field.onChange("out") } },
            ]}
          />
        )}
      />

      {mobility !== "no" && (
        <Input
          label={<RequiredLabel>{mobility === "in" ? "Date d'arrivée" : "Date de départ"}</RequiredLabel>}
          hintText="JJ/MM/AAAA"
          nativeInputProps={{ ...register("mobility_date"), placeholder: "JJ/MM/AAAA", maxLength: 10 }}
          state={errors.mobility_date ? "error" : "default"}
          stateRelatedMessage={errors.mobility_date?.message}
        />
      )}

      <Input
        label={<RequiredLabel>Nombre de jours covoiturés dans l'année</RequiredLabel>}
        nativeInputProps={{ ...register("days"), maxLength: 3, required: true }}
        state={errors.days ? "error" : "default"}
        stateRelatedMessage={errors.days?.message}
      />

      <Controller
        control={control}
        name="home_address"
        render={({ field, fieldState }) => (
          <AddressAutocomplete
            label={<RequiredLabel>Adresse du domicile</RequiredLabel>}
            arialabel="Adresse du domicile"
            value={field.value ?? ""}
            onChange={field.onChange}
            onBlur={field.onBlur}
            state={fieldState.error ? "error" : "default"}
            stateRelatedMessage={fieldState.error?.message}
          />
        )}
      />

      <Controller
        control={control}
        name="work_address"
        render={({ field, fieldState }) => (
          <AddressAutocomplete
            label={<RequiredLabel>Lieu de travail</RequiredLabel>}
            arialabel="Lieu de travail"
            value={field.value ?? ""}
            onChange={field.onChange}
            onBlur={field.onBlur}
            state={fieldState.error ? "error" : "default"}
            stateRelatedMessage={fieldState.error?.message}
          />
        )}
      />

      <Controller
        control={control}
        name="certifications"
        render={({ field, fieldState }) => (
          <Checkbox
            legend="Je certifie que…"
            options={CERTIFICATIONS.map((c) => ({
              label: certificationLabel(c.id, c.label),
              hintText:
                c.id === "checkbox_6"
                  ? 
                    <p className="fr-highlight fr-highlight--blue-ecume fr-mt-1w">Les agents de la fonction publique hospitalière ne peuvent pas bénéficier du Forfait mobilités durables, s'il existe des transports collectifs gratuits sur leur territoires, contrairement aux agents des fonctions publiques d'Etat et territoriale, qui ont désormais ce droit.</p>
                  : undefined,
              nativeInputProps: {
                value: c.id,
                checked: (field.value ?? []).includes(c.id),
                onChange: (e) =>
                  field.onChange(
                    e.target.checked
                      ? [...(field.value ?? []), c.id]
                      : (field.value ?? []).filter((id: string) => id !== c.id),
                  ),
              },
            }))}
            state={fieldState.error ? "error" : "default"}
            stateRelatedMessage={fieldState.error?.message}
          />
        )}
      />

      <h4>Signature</h4>

      <Input
        label={<RequiredLabel>Fait à</RequiredLabel>}
        nativeInputProps={{ ...register("location"), required: true }}
        state={errors.location ? "error" : "default"}
        stateRelatedMessage={errors.location?.message}
      />

      {submitError && (
        <p role="alert" style={{ color: "var(--text-default-error)" }}>
          La génération de l&apos;attestation a échoué. Veuillez réessayer.
        </p>
      )}

      <div className="fr-btns-group fr-btns-group--inline">
        <Button type="reset" priority="secondary">
          Annuler
        </Button>
        <Button type="submit" disabled={!isValid}>
          Générer
        </Button>
      </div>
    </form>
  );
}
