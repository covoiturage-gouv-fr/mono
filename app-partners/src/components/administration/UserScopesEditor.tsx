"use client";
import { fr } from "@codegouvfr/react-dsfr";
import TerritoryAutocomplete from "@/components/common/TerritoryAutocomplete";
import { type TerritoryOption } from "@/helpers/api";
import { type UserScopeInput } from "@/interfaces/dataInterface";
import Button from "@codegouvfr/react-dsfr/Button";
import Table from "@codegouvfr/react-dsfr/Table";

// Édition des périmètres territoire d'un user : liste + radio défaut + ajout (registry.admin).
export default function UserScopesEditor(props: {
  scopes: UserScopeInput[];
  onChange: (scopes: UserScopeInput[]) => void;
}) {
  const { scopes, onChange } = props;

  const nameOf = (s: UserScopeInput) => s.territory_name ?? `#${s.territory_id}`;

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

  const add = (t: TerritoryOption | null) => {
    if (!t || scopes.some((s) => s.territory_id === t._id)) return;
    onChange([
      ...scopes,
      { territory_id: t._id, is_default: scopes.length === 0, territory_name: t.name, siret: t.siret },
    ]);
  };

  const rows = scopes.map((s) => [
    nameOf(s),
    <input
      key={`def-${s.territory_id}`}
      type="radio"
      name="scope-default"
      aria-label={`Géographie par défaut : ${nameOf(s)}`}
      checked={!!s.is_default}
      onChange={() => setDefault(s.territory_id)}
    />,
    <Button
      key={`rm-${s.territory_id}`}
      iconId="fr-icon-delete-bin-line"
      priority="tertiary no outline"
      size="small"
      title="Retirer la géographie"
      disabled={scopes.length <= 1}
      onClick={() => remove(s.territory_id)}
    >
      Retirer
    </Button>,
  ]);

  return (
    <>
      <TerritoryAutocomplete
        value={null}
        label="Ajouter une géographie"
        excludeIds={scopes.map((s) => s.territory_id).filter((id): id is number => !!id)}
        onChange={add}
      />
      <Table data={rows} headers={["Territoire", "Défaut", "Action"]} fixed className={fr.cx("fr-mb-0")} />
    </>
  );
}
