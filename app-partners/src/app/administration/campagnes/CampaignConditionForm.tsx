"use client";
import SelectGeo from "@/components/common/SelectGeo";
import {
  classesPreuve,
  eligibiliteLabels,
  eligibilites,
  emptyCampaignCondition,
  type CampaignConditionForm as FormValues,
  campaignConditionSchema,
  sensTrajets,
  sensTrajetsLabels,
} from "@/helpers/campaignCondition";
import { castPerimeterType } from "@/helpers/search";
import Autocomplete from "@mui/material/Autocomplete";
import TextField from "@mui/material/TextField";
import { fr } from "@codegouvfr/react-dsfr";
import ButtonsGroup from "@codegouvfr/react-dsfr/ButtonsGroup";
import { Checkbox } from "@codegouvfr/react-dsfr/Checkbox";
import { Input } from "@codegouvfr/react-dsfr/Input";
import { Select } from "@codegouvfr/react-dsfr/Select";
import { ToggleSwitch } from "@codegouvfr/react-dsfr/ToggleSwitch";
import { type ReactNode, useMemo, useState } from "react";

type Draft = ReturnType<typeof emptyCampaignCondition>;
type Key = keyof Draft;

export default function CampaignConditionForm(props: {
  initial: Draft;
  operators: { id: number; name: string }[];
  // Compte opérateur : son entreprise est renseignée d'office, le choix des opérateurs est masqué.
  lockedOperatorId: number | null;
  // Compte territoire : son territoire est imposé (l'API le recalcule), la recherche est masquée.
  lockedTerritoryLabel: string | null;
  onSubmit: (values: FormValues) => void;
  onCancel: () => void;
}) {
  const [draft, setDraft] = useState<Draft>(props.initial);
  const [submitted, setSubmitted] = useState(false);
  const errors = useMemo(() => {
    const r = campaignConditionSchema.safeParse(draft);
    return r.success ? {} : r.error.flatten().fieldErrors;
  }, [draft]);
  const shown = (k: Key) => (submitted ? errors[k as keyof typeof errors]?.[0] : undefined);

  const set = <K extends Key>(k: K, v: Draft[K]) => setDraft((d) => ({ ...d, [k]: v }));
  const text = (k: Key) => (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) =>
    set(k, (e.target.value === "" ? null : e.target.value) as never);
  const num = (k: Key) => (e: React.ChangeEvent<HTMLInputElement>) =>
    set(k, (e.target.value === "" ? null : Number(e.target.value)) as never);

  const field = (k: Key, label: string, kind: "euros" | "entier" | "km" | "date" | "texte", hint?: string) => {
    const error = shown(k);
    const value = draft[k] as string | number | null;
    const isNum = kind === "euros" || kind === "entier" || kind === "km";
    return (
      <Input
        // Unité dans le libellé : un sous-titre décalerait les champs voisins d'une même ligne.
        label={kind === "euros" ? `${label} (€)` : kind === "km" ? `${label} (km)` : label}
        hintText={hint}
        state={error ? "error" : "default"}
        stateRelatedMessage={error}
        nativeInputProps={{
          type: isNum ? "number" : kind === "date" ? "date" : "text",
          min: isNum ? 0 : undefined,
          step: kind === "euros" ? "0.01" : isNum ? 1 : undefined,
          value: value ?? "",
          onChange: isNum ? num(k) : text(k),
        }}
      />
    );
  };

  const eligibilite = (k: "passager_eligible_gratuite" | "passager_eligibilite_reduction", label: string) => (
    <Select
      label={label}
      state={shown(k) ? "error" : "default"}
      stateRelatedMessage={shown(k)}
      nativeSelectProps={{
        value: draft[k] ?? "",
        onChange: (e) => set(k, (e.target.value || null) as Draft[typeof k]),
      }}
    >
      <option value="">Sélectionner</option>
      {eligibilites.map((e) => (
        <option key={e} value={e}>
          {eligibiliteLabels[e]}
        </option>
      ))}
    </Select>
  );

  const toggle = (
    k: "premiere_campagne" | "passager_gratuite" | "passager_reduction_ticket" | "zone_exclusion",
    label: string,
  ) => (
    <ToggleSwitch
      label={label}
      inputTitle={label}
      checked={draft[k]}
      onChange={(v) => set(k, v)}
      showCheckedHint={false}
    />
  );

  return (
    <form
      onSubmit={(e) => {
        e.preventDefault();
        setSubmitted(true);
        const r = campaignConditionSchema.safeParse(draft);
        if (r.success) props.onSubmit(draft as FormValues);
      }}
      noValidate
    >
      {submitted && Object.keys(errors).length > 0 && (
        <p className={fr.cx("fr-error-text", "fr-mb-3w")}>
          Le formulaire contient des erreurs, corrigez les champs signalés.
        </p>
      )}

      <Section title="1. Territoire et opérateurs">
        {props.lockedTerritoryLabel !== null ? (
          <p className={fr.cx("fr-mb-1w")}>
            Territoire : <strong>{props.lockedTerritoryLabel}</strong>
          </p>
        ) : (
          <>
            {draft.territory && (
              <p className={fr.cx("fr-mb-1w")}>
                Territoire : <strong>{draft.territory.label}</strong> ({castPerimeterType(draft.territory.type)}{" "}
                {draft.territory.code})
              </p>
            )}
            <SelectGeo
              onChange={(o) => set("territory", o ? { type: o.type, code: o.territory, label: o.l_territory } : null)}
            />
            {shown("territory") && <p className={fr.cx("fr-error-text")}>{shown("territory")}</p>}
          </>
        )}
        {props.lockedOperatorId === null && (
          <div className={fr.cx("fr-mt-3w", "fr-mb-3w")}>
            <Autocomplete
              multiple
              id="select-operators"
              options={props.operators}
              getOptionLabel={(o) => o.name}
              isOptionEqualToValue={(a, b) => a.id === b.id}
              value={props.operators.filter((o) => draft.operator_ids.includes(o.id))}
              onChange={(_e, selected) =>
                set(
                  "operator_ids",
                  selected.map((o) => o.id),
                )
              }
              noOptionsText="Aucun opérateur"
              filterSelectedOptions
              renderInput={(params) => (
                <TextField
                  {...params}
                  label="Opérateurs participants"
                  placeholder={draft.operator_ids.length ? "" : "Chercher un opérateur"}
                  error={!!shown("operator_ids")}
                  helperText={shown("operator_ids") ?? "Plusieurs opérateurs possibles"}
                />
              )}
            />
          </div>
        )}
      </Section>

      <Section title="2. Période et budget">
        {toggle("premiere_campagne", "Première campagne sur ce territoire")}
        <Row>
          {field("date_debut", "Date de début", "date")}
          {field("date_fin", "Date de fin (facultative)", "date")}
        </Row>
        {field("budget_incitations", "Budget des incitations", "euros")}
      </Section>

      <Section title="3. Conditions conducteur">
        <Row>
          {field("conducteur_montant_min_par_passager", "Montant minimum par passager", "euros")}
          {field("conducteur_montant_max_par_passager", "Montant maximum par passager", "euros")}
        </Row>
        <Row>
          {field("conducteur_montant_max_par_mois", "Plafond mensuel", "euros")}
          {field("conducteur_trajets_max_par_mois", "Trajets maximum par mois", "entier")}
        </Row>
      </Section>

      <Section title="4. Conditions passager">
        {toggle("passager_gratuite", "Trajet gratuit pour le passager")}
        {draft.passager_gratuite && eligibilite("passager_eligible_gratuite", "Qui bénéficie de la gratuité ?")}
        {toggle("passager_reduction_ticket", "Réduction sur le prix du trajet")}
        {draft.passager_reduction_ticket && (
          <Row>
            {eligibilite("passager_eligibilite_reduction", "Qui bénéficie de la réduction ?")}
            {field("passager_montant_ticket", "Montant de la réduction", "euros")}
          </Row>
        )}
        {field("passager_trajets_max_par_mois", "Trajets maximum par mois", "entier")}
      </Section>

      <Section title="5. Zone et trajets">
        <Select
          label="Sens des trajets"
          nativeSelectProps={{
            value: draft.zone_sens_des_trajets ?? "",
            onChange: (e) => set("zone_sens_des_trajets", (e.target.value || null) as Draft["zone_sens_des_trajets"]),
          }}
        >
          <option value="">Sélectionner</option>
          {sensTrajets.map((s) => (
            <option key={s} value={s}>
              {sensTrajetsLabels[s]}
            </option>
          ))}
        </Select>
        {toggle("zone_exclusion", "Des zones sont exclues")}
        {draft.zone_exclusion && field("zone_exclusion_liste", "Zones exclues", "texte", "Séparées par des virgules")}
        {field("autre_exclusion", "Autres exclusions", "texte")}
        <Row>
          {field("trajet_longueur_min", "Longueur minimale", "km")}
          {field("trajet_longueur_max", "Longueur maximale", "km")}
        </Row>
        <Checkbox
          legend="Classes de preuve acceptées"
          orientation="horizontal"
          state={shown("trajet_classe_preuve") ? "error" : "default"}
          stateRelatedMessage={shown("trajet_classe_preuve")}
          options={classesPreuve.map((c) => ({
            label: `Classe ${c}`,
            nativeInputProps: {
              checked: draft.trajet_classe_preuve.includes(c),
              onChange: (e) =>
                set(
                  "trajet_classe_preuve",
                  e.target.checked
                    ? [...draft.trajet_classe_preuve, c]
                    : draft.trajet_classe_preuve.filter((x) => x !== c),
                ),
            },
          }))}
        />
      </Section>

      <Section title="6. Informations complémentaires">
        {field("lien_page_collectivite", "Lien vers la page de la collectivité", "texte", "https://…")}
        <Input
          label="Autres informations"
          textArea
          state={shown("autres_informations") ? "error" : "default"}
          stateRelatedMessage={shown("autres_informations")}
          nativeTextAreaProps={{ value: draft.autres_informations ?? "", onChange: text("autres_informations") }}
        />
      </Section>

      <ButtonsGroup
        inlineLayoutWhen="always"
        alignment="right"
        buttons={[
          { children: "Annuler", priority: "secondary", type: "button", onClick: props.onCancel },
          { children: "Enregistrer en brouillon", type: "submit" },
        ]}
      />
    </form>
  );
}

function Section(props: { title: string; children: ReactNode }) {
  return (
    <fieldset className={fr.cx("fr-fieldset", "fr-mb-4w")} style={{ display: "block" }}>
      <legend className={fr.cx("fr-h5")}>{props.title}</legend>
      {props.children}
    </fieldset>
  );
}

function Row(props: { children: ReactNode }) {
  return (
    <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(16rem, 1fr))", columnGap: "1.5rem" }}>
      {props.children}
    </div>
  );
}
