"use client";
import { searchTerritories, type TerritorySearchResult } from "@/helpers/api";
import { castPerimeterType } from "@/helpers/search";
import { type PerimeterType } from "@/interfaces/searchInterface";
import { fr } from "@codegouvfr/react-dsfr";
import Tag from "@codegouvfr/react-dsfr/Tag";
import Autocomplete from "@mui/material/Autocomplete";
import TextField from "@mui/material/TextField";
import { debounce } from "@mui/material/utils";
import { useEffect, useMemo, useRef, useState } from "react";

export default function SelectGeo(props: {
  defaultValue?: string;
  onChange: (
    option: {
      id: string;
      territory: string;
      l_territory: string;
      type: PerimeterType;
    } | null,
  ) => void;
}) {
  const defaultOption: TerritorySearchResult = {
    id: "",
    territory: "",
    l_territory: "",
    type: "com", // Default type, can be changed based on your needs
    year: 0,
  };
  const [, setValue] = useState<TerritorySearchResult | null>(null);
  const [options, setOptions] = useState<TerritorySearchResult[]>([defaultOption]);
  // Une réponse lente ne doit pas écraser celle d'une frappe plus récente :
  // chaque recherche annule la précédente.
  const pending = useRef<AbortController>(undefined);
  const search = useMemo(
    () =>
      debounce((v: string | null) => {
        pending.current?.abort();
        const { signal } = (pending.current = new AbortController());
        void searchTerritories(v, 20, signal).then((results) => {
          if (!signal.aborted) setOptions(results);
        });
      }, 300),
    [],
  );
  // Démontage : le `search` en cours ne doit pas tirer après coup.
  useEffect(
    () => () => {
      search.clear();
      pending.current?.abort();
    },
    [search],
  );

  return (
    <>
      <Autocomplete
        id="select-territory"
        options={options}
        getOptionLabel={(option) => `${option.l_territory} - ${castPerimeterType(option.type)}`}
        renderOption={(props, option) => {
          return (
            <li {...props} key={option.id}>
              <div>
                <div>
                  {" "}
                  <span className={fr.cx("fr-text--bold")}>{option.l_territory}</span>{" "}
                  <span className={fr.cx("fr-text--xs")}>({option.territory})</span>
                </div>
                <div>
                  <Tag small>{castPerimeterType(option.type)}</Tag>
                </div>
              </div>
            </li>
          );
        }}
        noOptionsText={"Pas de résultats"}
        renderInput={(params) => <TextField {...params} label="Chercher un territoire" />}
        filterOptions={(options) => options}
        onInputChange={(e, v, reason) => {
          // `reset` = libellé réinjecté après sélection, pas une saisie : ne pas rechercher.
          if (reason !== "input") return;
          if (!v.trim()) {
            search.clear();
            pending.current?.abort();
            setOptions([defaultOption]);
            return;
          }
          search(v);
        }}
        onChange={(e, v) => {
          setValue(v);
          props.onChange(v);
        }}
      />
    </>
  );
}
