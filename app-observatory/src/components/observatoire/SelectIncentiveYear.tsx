"use client";
import { getUrl } from "@/helpers/search";
import { INSEECode, PerimeterType } from "@/interfaces/observatoire/Perimeter";
import { fr } from "@codegouvfr/react-dsfr";
import { useRouter } from "next/navigation";

export default function SelectIncentiveYear(props: {
  params: { code: INSEECode; type: PerimeterType; year: string | null };
  url: string;
}) {
  const router = useRouter();
  const list = [
    { val: undefined, name: "En cours" },
    { val: "2024", name: "2024" },
    { val: "2025", name: "2025" },
    { val: "2026", name: "2026" },
  ];
  const currentYear = props.params.year ? props.params.year : undefined;
  const onClick = (v: string | undefined) => {
    router.push(
      `${getUrl(props.url, {
        territory: props.params.code,
        type: props.params.type,
        l_territory: "",
      })}${v !== undefined ? `&year=${v}` : ""}`,
    );
  };

  return (
    <fieldset className={fr.cx("fr-segmented", "fr-mb-5v")}>
      <legend className={fr.cx("fr-segmented__legend")}>
        Filtrer par année
      </legend>
      <div className={fr.cx("fr-segmented__elements")}>
        {list.map((l, i) => {
          return (
            <div
              key={i}
              className={fr.cx("fr-segmented__element")}
              onClick={() => onClick(l.val)}
            >
              <input
                value={l.val}
                name="year"
                type="radio"
                checked={currentYear === l.val}
                readOnly
              />
              <label className={fr.cx("fr-label")}>{l.name}</label>
            </div>
          );
        })}
      </div>
    </fieldset>
  );
}
