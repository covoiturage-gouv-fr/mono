"use client";
import { castPerimeterType, fetchSearchAPI } from "@/helpers/search";
import { type PerimeterType } from "@/interfaces/searchInterface";
import { fr } from "@codegouvfr/react-dsfr";
import Tag from "@codegouvfr/react-dsfr/Tag";
import Autocomplete from "@mui/material/Autocomplete";
import TextField from "@mui/material/TextField";
import { useState } from "react";

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
  const defaultOption = {
    id: "",
    territory: "",
    l_territory: "",
    type: "com" as PerimeterType, // Default type, can be changed based on your needs
  };
  const [, setValue] = useState<typeof defaultOption | null>(null);
  const [options, setOptions] = useState<(typeof defaultOption)[]>([defaultOption]);
  const search = async (v: string | null) => {
    const query = {
      q: v,
      attributesToSearchOn: ["territory", "l_territory"],
      filter: "is_latest = true",
      limit: 20,
    };
    const response = await fetchSearchAPI<{ hits: (typeof defaultOption)[] }>("indexes/geo/search", {
      method: "post",
      body: JSON.stringify(query),
    });
    setOptions(response.hits);
  };

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
        onInputChange={(e, v) => {
          void search(v)
            .then(() => {
              console.log("search done");
            })
            .catch(console.error);
        }}
        onChange={(e, v) => {
          setValue(v);
          props.onChange(v);
        }}
      />
    </>
  );
}
