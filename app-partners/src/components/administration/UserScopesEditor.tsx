"use client";
import { type Territory } from "@/interfaces/dataInterface";
import { type UserScopeInput } from "@/interfaces/dataInterface";
import Button from "@codegouvfr/react-dsfr/Button";
import Select from "@codegouvfr/react-dsfr/Select";
import Table from "@codegouvfr/react-dsfr/Table";

// Édition des périmètres territoire d'un user : liste + radio défaut + ajout (registry.admin).
export default function UserScopesEditor(props: {
  scopes: UserScopeInput[];
  territories: Territory[];
  onChange: (scopes: UserScopeInput[]) => void;
}) {
  const { scopes, territories, onChange } = props;

  const nameOf = (territory_id?: number) => territories.find((t) => t?._id === territory_id)?.name ?? territory_id;

  const setDefault = (territory_id?: number) => {
    onChange(scopes.map((s) => ({ ...s, is_default: s.territory_id === territory_id })));
  };

  const remove = (territory_id?: number) => {
    const next = scopes.filter((s) => s.territory_id !== territory_id);
    // Retrait du défaut : promotion du premier restant, sans muter les objets partagés avec la ligne en cache.
    if (next.length > 0 && !next.some((s) => s.is_default)) {
      return onChange(next.map((s, i) => ({ ...s, is_default: i === 0 })));
    }
    onChange(next);
  };

  const add = (territory_id: number) => {
    if (!territory_id || scopes.some((s) => s.territory_id === territory_id)) return;
    onChange([...scopes, { territory_id, is_default: scopes.length === 0 }]);
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

  const options = territories.filter((t) => t?._id && !scopes.some((s) => s.territory_id === t._id));

  return (
    <>
      <Table data={rows} headers={["Territoire", "Défaut", "Action"]} fixed />
      {/* Select natif plutôt qu'un champ à liste déroulante : la liste d'un composant porté
          hors de la modale se peint sous elle, et le rendu s'aligne sur le reste du formulaire. */}
      <Select
        label="Ajouter un périmètre"
        disabled={options.length === 0}
        nativeSelectProps={{
          id: "add-scope",
          value: "",
          onChange: (e) => add(Number(e.target.value)),
        }}
      >
        <option value="">{options.length ? "Sélectionner un territoire" : "Aucun territoire disponible"}</option>
        {options.map((t) => (
          <option key={t._id} value={t._id}>
            {t.name}
          </option>
        ))}
      </Select>
    </>
  );
}
