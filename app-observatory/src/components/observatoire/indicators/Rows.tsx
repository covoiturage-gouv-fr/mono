import { RowsProps } from "@/interfaces/observatoire/componentsInterfaces";
import { fr } from "@codegouvfr/react-dsfr";
import Analyse from "./Analyse";
import Graph from "./Graph";
import Indicator from "./Indicator";
import Map from "./Map";

export default function Rows(props: RowsProps) {
  return (
    <>
      {props.data.length > 0 && (
        <div
          className={fr.cx("fr-grid-row", "fr-grid-row--gutters", "fr-mt-5w")}
        >
          {props.data.map((r, i: number) => {
            switch (r.__component) {
              case "row.title":
                return (
                  <div key={i} className={fr.cx("fr-col-12")}>
                    <h2>{r.title}</h2>
                  </div>
                );
              case "row.indicator":
                return (
                  <Indicator
                    key={i}
                    value={r.value}
                    unit={r.unit}
                    info={r.info}
                    text={r.text}
                    icon={r.icon}
                    link={r.link}
                  />
                );
              case "row.analyse":
                return (
                  <Analyse
                    key={i}
                    title={r.title}
                    content={r.content}
                    link={r.link}
                  />
                );
              case "row.map":
                return (
                  <div key={i} className={fr.cx("fr-col")}>
                    <Map title={r.title} params={r.params} />
                  </div>
                );
              case "row.graph":
                return (
                  <div key={i} className={fr.cx("fr-col")}>
                    <Graph title={r.title} params={r.params} />
                  </div>
                );
            }
          })}
        </div>
      )}
    </>
  );
}
