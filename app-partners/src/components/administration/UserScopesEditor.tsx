"use client";
import { type Territory } from "@/interfaces/dataInterface";
import { type UserScopeInput } from "@/interfaces/dataInterface";
import { fr } from "@codegouvfr/react-dsfr";
import Button from "@codegouvfr/react-dsfr/Button";
import Table from "@codegouvfr/react-dsfr/Table";
import Autocomplete from "@mui/material/Autocomplete";
import TextField from "@mui/material/TextField";
import { useState } from "react";

// Édition des périmètres territoire d'un user : liste + radio défaut + ajout (registry.admin).
export default function UserScopesEditor(props: {
  scopes: UserScopeInput[];
  territories: Territory[];
  onChange: (scopes: UserScopeInput[]) => void;
}) {
  const { scopes, territories, onChange } = props;
  const [toAdd, setToAdd] = useState<Territory | null>(null);

  const nameOf = (territory_id?: number) => territories.find((t) => t._id === territory_id)?.name ?? territory_id;

  const setDefault = (territory_id?: number) => {
    onChange(scopes.map((s) => ({ ...s, is_default: s.territory_id === territory_id })));
  };

  const remove = (territory_id?: number) => {
    const next = scopes.filter((s) => s.territory_id !== territory_id);
    // Retrait du défaut : promotion automatique du premier périmètre restant.
    if (next.length > 0 && !next.some((s) => s.is_default)) {
      next[0].is_default = true;
    }
    onChange(next);
  };

  const add = (territory: Territory | null) => {
    if (!territory?._id || scopes.some((s) => s.territory_id === territory._id)) return;
    onChange([...scopes, { territory_id: territory._id, is_default: scopes.length === 0 }]);
    setToAdd(null);
  };

  const rows = scopes.map((s) => [
    nameOf(s.territory_id),
    <input
      key={`def-${s.territory_id}`}
      type="radio"
      name="scope-default"
      aria-label={`Périmètre par défaut : ${String(nameOf(s.territory_id))}`}
      checked={!!s.is_default}
      onChange={() => setDefault(s.territory_id)}
    />,
    <Button
      key={`rm-${s.territory_id}`}
      iconId="fr-icon-delete-bin-line"
      priority="tertiary no outline"
      size="small"
      title="Retirer le périmètre"
      disabled={scopes.length <= 1}
      onClick={() => remove(s.territory_id)}
    >
      Retirer
    </Button>,
  ]);

  const options = territories.filter((t) => !scopes.some((s) => s.territory_id === t._id));

  return (
    <div className={fr.cx("fr-mt-2w")}>
      <Table data={rows} headers={["Territoire", "Défaut", "Action"]} fixed />
      <Autocomplete
        id="add-scope"
        size="small"
        options={options}
        value={toAdd}
        getOptionLabel={(o) => o.name}
        isOptionEqualToValue={(o, v) => o._id === v._id}
        noOptionsText="Aucun territoire"
        onChange={(e, v) => add(v)}
        renderInput={(params) => <TextField {...params} label="Ajouter un périmètre" />}
      />
    </div>
  );
}
