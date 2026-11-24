"use client";
import { useState } from "react";
import SearchSelect from "@/components/common/SearchSelect";
import { useOperatorsList } from "@/hooks/api";

export default function SelectOperator(props: {
  defaultValue: number | null;
  onChange: (id: number | null, label?: string) => void;
}) {
  const [value, setValue] = useState<number | null>(props.defaultValue);
  const { data } = useOperatorsList({ limit: 500 });
  return (
    <SearchSelect
      label="Opérateur"
      noOptionsText="Aucun opérateur"
      options={(data?.data ?? []).flatMap((d) =>
        d.id === undefined ? [] : [{ id: d.id, label: d.name }],
      )}
      value={value}
      onChange={(id, label) => {
        setValue(id);
        props.onChange(id, label);
      }}
    />
  );
}
