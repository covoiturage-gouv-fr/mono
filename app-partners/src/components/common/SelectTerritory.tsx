"use client";
import TerritoryAutocomplete from "@/components/common/TerritoryAutocomplete";
import type { TerritoryOption } from "@/helpers/api";
import { useTerritoriesList } from "@/hooks/api";
import { useState } from "react";

// Même identifiant (`territory_group._id`) que `user.territory_id` : la route
// `dashboard/territories` sert à la fois la recherche et la résolution du libellé initial.
export default function SelectTerritory(props: {
  defaultValue: number | null;
  onChange: (id: number | null, label?: string) => void;
}) {
  const [id, setId] = useState<number | null>(props.defaultValue);
  const [picked, setPicked] = useState<TerritoryOption | null>(null);
  const [prevDefaultValue, setPrevDefaultValue] = useState(props.defaultValue);
  if (prevDefaultValue !== props.defaultValue) {
    setPrevDefaultValue(props.defaultValue);
    setId(props.defaultValue);
  }
  const { data } = useTerritoriesList({ limit: 1, territory_id: id });

  const fetched = data?.data[0];
  let value: TerritoryOption | null = null;
  if (picked?._id === id) value = picked;
  else if (id && fetched?._id === id)
    value = { _id: id, name: fetched.name, siret: fetched.siret };

  return (
    <TerritoryAutocomplete
      label="Territoire"
      value={value}
      onChange={(t) => {
        setPicked(t);
        setId(t?._id ?? null);
        props.onChange(t?._id ?? null, t?.name);
      }}
    />
  );
}
