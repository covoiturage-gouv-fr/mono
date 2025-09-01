"use client";
import { Config } from "@/config";
import { useApi } from "@/hooks/useApi";
import { type OperatorsInterface } from "@/interfaces/dataInterface";
import { Select } from "@codegouvfr/react-dsfr/Select";
import { useState } from "react";

export default function SelectOperator(props: { defaultValue?: number; onChange: (id?: number) => void }) {
  const [value, setValue] = useState<number | undefined>(props.defaultValue);
  const url = `${Config.get<string>("next.public_api_url", "")}/v3/dashboard/operators?limit=100`;
  const { data } = useApi<OperatorsInterface>(url, true);
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
      <option value="">Selectionner un operateur</option>
      {data?.data.map((d, i) => (
        <option key={i} value={d.id}>
          {d.name}
        </option>
      ))}
    </Select>
  );
}
