import { IndicatorProps } from "@/interfaces/observatoire/componentsInterfaces";
import { fr } from "@codegouvfr/react-dsfr";
import { Badge } from "@codegouvfr/react-dsfr/Badge";
import Link from "next/link";
import style from "./Indicator.module.scss";

const COL_MD = {
  3: "fr-col-md-3",
  4: "fr-col-md-4",
  6: "fr-col-md-6",
} as const;

export default function Indicator(props: IndicatorProps) {
  return (
    <div
      className={`${fr.cx("fr-col-12", COL_MD[props.md ?? 3])} ${style.col}`}
    >
      <div className={`${fr.cx("fr-callout")} ${style.stat}`}>
        {props.info && <Badge severity="info">{props.info}</Badge>}

        <div className={`fr-callout__title`}>
          <p className={`${style.value}`}>
            {props.icon && (
              <span
                aria-hidden={true}
                className={`${props.icon} ${style.icon}`}
              ></span>
            )}
            <span className={`fr-h3`}>
              {props.value} {props.unit ? props.unit : ""}
            </span>
          </p>
        </div>
        <div className={`fr-callout__text ${style.text}`}>
          {props.link && (
            <Link
              href={`${props.link}`}
              target="_blank"
              rel="noopener noreferrer"
            >
              {props.text}
            </Link>
          )}
          {!props.link && <>{props.text}</>}
        </div>
        {props.note && <p className={style.note}>{props.note}</p>}
        {props.items && props.items.length > 0 && (
          <ul className={style.list}>
            {props.items.map((item, i) => (
              <li key={i}>{item}</li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}
