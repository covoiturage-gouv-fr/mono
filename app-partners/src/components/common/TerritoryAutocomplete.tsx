"use client";
import { searchDashboardTerritories, type TerritoryOption } from "@/helpers/api";
import Autocomplete from "@mui/material/Autocomplete";
import TextField from "@mui/material/TextField";
import { debounce } from "@mui/material/utils";
import { useEffect, useMemo, useState } from "react";

const MIN_CHARS = 2;
const RESULTS_LIMIT = 20;

export const territoryLabel = (t: TerritoryOption) => (t.siret ? `${t.name} (${t.siret})` : t.name);

// Recherche serveur d'un territoire (`territory_group`) pour la modale utilisateur.
// `value` null + sélection = mode « ajout » : le champ se vide après chaque choix.
export default function TerritoryAutocomplete(props: {
  value: TerritoryOption | null;
  onChange: (territory: TerritoryOption | null) => void;
  excludeIds?: number[];
  label: string;
  disabled?: boolean;
}) {
  const { value, onChange, excludeIds, label, disabled } = props;
  const valueLabel = value ? territoryLabel(value) : "";
  const [options, setOptions] = useState<TerritoryOption[]>([]);
  const [input, setInput] = useState(valueLabel);
  // Une réponse lente ne doit pas écraser celle d'une frappe plus récente :
  // chaque recherche annule la précédente.
  const search = useMemo(() => {
    const pending: { controller?: AbortController } = {};
    const run = debounce((v: string) => {
      pending.controller?.abort();
      const { signal } = (pending.controller = new AbortController());
      void searchDashboardTerritories(v, RESULTS_LIMIT, signal).then((results) => {
        if (!signal.aborted) setOptions(results);
      });
    }, 300);
    const cancel = () => {
      run.clear();
      pending.controller?.abort();
    };
    return { run, cancel };
  }, []);
  useEffect(() => search.cancel, [search]);
  // Clé = libellé, pas l'objet : le parent peut reconstruire `value` à chaque rendu.
  const [prevValueLabel, setPrevValueLabel] = useState(valueLabel);
  if (prevValueLabel !== valueLabel) {
    setPrevValueLabel(valueLabel);
    setInput(valueLabel);
  }

  const stop = () => {
    search.cancel();
    setOptions([]);
  };

  const visible = options.filter((o) => !excludeIds?.includes(o._id));
  const tooShort = input.trim().length < MIN_CHARS;

  return (
    <Autocomplete
      value={value}
      inputValue={input}
      options={visible}
      disabled={disabled}
      // Porté hors de la modale, le menu se peignait sous le `<dialog>`.
      disablePortal
      getOptionLabel={territoryLabel}
      isOptionEqualToValue={(a, b) => a._id === b._id}
      filterOptions={(o) => o}
      noOptionsText={tooShort ? `Saisissez au moins ${MIN_CHARS} caractères` : "Aucun territoire"}
      renderInput={(params) => <TextField {...params} label={label} />}
      onInputChange={(e, v, reason) => {
        setInput(v);
        // `reset` = libellé réinjecté après sélection, pas une saisie : ne pas rechercher.
        if (reason !== "input") return;
        if (v.trim().length < MIN_CHARS) return stop();
        search.run(v);
      }}
      onChange={(e, v) => {
        stop();
        setInput(v && v._id === value?._id ? territoryLabel(v) : "");
        onChange(v);
      }}
    />
  );
}
