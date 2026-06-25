"use client";

import { AddressAutocomplete } from "@/components/AddressAutocomplete";
import { RequiredLabel } from "@/components/RequiredLabel";
import { saveHonor } from "@/services/counter.service";
import { generateLimitedPdf } from "@/services/pdfLimited.service";
import { LimitedFormValues, limitedFormSchema } from "@/shared/schemas/limitedForm.schema";
import { Button } from "@codegouvfr/react-dsfr/Button";
import { Input } from "@codegouvfr/react-dsfr/Input";
import { zodResolver } from "@hookform/resolvers/zod";
import { useEffect, useState } from "react";
import { Controller, useForm } from "react-hook-form";

const STORAGE_KEY = "formLtd";

export function LimitedForm() {
  const [submitError, setSubmitError] = useState(false);
  const {
    register,
    handleSubmit,
    reset,
    watch,
    control,
    formState: { errors, isValid },
  } = useForm<LimitedFormValues>({
    resolver: zodResolver(limitedFormSchema),
    mode: "onChange",
  });

  // reload saved data in a crash free way (client-only: localStorage is undefined on the server)
  useEffect(() => {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      if (saved) reset(JSON.parse(saved));
    } catch {
      localStorage.removeItem(STORAGE_KEY);
    }
  }, [reset]);

  // auto-save (debounced) on every change
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

  const onSubmit = async (data: LimitedFormValues) => {
    try {
      setSubmitError(false);
      await generateLimitedPdf(data);
      saveHonor("limited", data.employer);
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

  return (
    <form onSubmit={handleSubmit(onSubmit)} onReset={onReset}>
      <h4>Secteur privé</h4>
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

      <Controller
        control={control}
        name="address"
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

      <Input
        label={<RequiredLabel>Nom de l'employeur</RequiredLabel>}
        nativeInputProps={{ ...register("employer"), required: true }}
        state={errors.employer ? "error" : "default"}
        stateRelatedMessage={errors.employer?.message}
      />

      <Input
        label={<RequiredLabel>Quotité de temps de travail (en pourcentage. 100 = temps plein)</RequiredLabel>}
        nativeInputProps={{ ...register("workshare"), required: true }}
        state={errors.workshare ? "error" : "default"}
        stateRelatedMessage={errors.workshare?.message}
      />

      <h4>Covoiturage</h4>

      <Input
        label="Nombre de kilomètres par trajet"
        hintText="Facultatif"
        nativeInputProps={{ ...register("distance"), maxLength: 6 }}
        state={errors.distance ? "error" : "default"}
        stateRelatedMessage={errors.distance?.message}
      />

      <Input
        label="Nombre de jours covoiturés dans l'année"
        hintText="Facultatif"
        nativeInputProps={{ ...register("days"), maxLength: 3 }}
        state={errors.days ? "error" : "default"}
        stateRelatedMessage={errors.days?.message}
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
