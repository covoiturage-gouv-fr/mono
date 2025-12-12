"use client";
import { useOperatorsList } from "@/hooks/api";
import { Select } from "@codegouvfr/react-dsfr/Select";
import { useState } from "react";

export default function SelectOperator(props: { defaultValue: number | null; onChange: (id: number | null) => void }) {
  const [value, setValue] = useState<number | null>(props.defaultValue);
  const { data } = useOperatorsList({ limit: 100 });
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
      <option value="">Sélectionner un opérateur</option>
      {data?.data.map((d, i) => (
        <option key={i} value={d.id}>
          {d.name}
        </option>
      ))}
    </Select>
  );
}
