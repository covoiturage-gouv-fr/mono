"use client";
import Autocomplete from "@mui/material/Autocomplete";
import TextField from "@mui/material/TextField";

export interface SearchOption {
  id: number;
  label: string;
}

// Casse, accents et ponctuation neutralisés : « ile de france » trouve « Île-de-France ».
export const normalize = (s: string) =>
  s
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();

// Liste déjà chargée en entier : filtre côté client.
export default function SearchSelect(props: {
  options: SearchOption[];
  value: number | null;
  onChange: (id: number | null, label?: string) => void;
  label: string;
  noOptionsText: string;
}) {
  const { options, value, onChange, label, noOptionsText } = props;
  return (
    // Un <fieldset> position: relative sert de repère au popper, mais Chrome y décale l'origine sous la <legend> : on ancre ailleurs.
    <div style={{ position: "relative" }}>
      <Autocomplete
        value={options.find((o) => o.id === value) ?? null}
        options={options}
        // Dans un <dialog> ou une carte, le menu porté hors du DOM passerait dessous.
        disablePortal
        filterOptions={(opts, { inputValue }) => {
          const q = normalize(inputValue);
          return opts.filter((o) => normalize(o.label).includes(q));
        }}
        getOptionLabel={(o) => o.label}
        isOptionEqualToValue={(a, b) => a.id === b.id}
        noOptionsText={noOptionsText}
        renderInput={(params) => <TextField {...params} label={label} />}
        onChange={(_, o) => onChange(o?.id ?? null, o?.label)}
      />
    </div>
  );
}
