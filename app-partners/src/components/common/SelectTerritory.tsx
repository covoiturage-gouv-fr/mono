"use client";
import { useTerritoriesList } from "@/hooks/api";
import { Select } from "@codegouvfr/react-dsfr/Select";
import { useEffect, useState } from "react";

export default function SelectTerritory(props: { defaultValue: number | null; onChange: (id: number | null) => void }) {
  const [value, setValue] = useState<number | null>(props.defaultValue);
  const { data } = useTerritoriesList({ limit: 200 });
  useEffect(() => {
    setValue(props.defaultValue);
  }, [props.defaultValue]);

  return (
    <Select
      label=""
      nativeSelectProps={{
        value: value ?? "",
        onChange: (e) => {
          const newValue = Number(e.target.value);
          setValue(newValue);
          props.onChange(newValue);
        },
      }}
    >
      <option value="">Selectionner un territoire</option>
      {data?.data.map((d, i) => (
        <option key={i} value={d._id}>
          {d.name}
        </option>
      ))}
    </Select>
  );
}
