import { fr } from "@codegouvfr/react-dsfr";

type StatFigureProps = {
  value: string;
  label: string;
  note?: string;
};

export default function StatFigure({ value, label, note }: StatFigureProps) {
  return (
    <div className={fr.cx("fr-callout")}>
      <p className={fr.cx("fr-callout__title", "fr-display--xs", "fr-mb-1w")}>
        {value}
      </p>
      <p className={fr.cx("fr-callout__text", "fr-text--lg", "fr-mb-0")}>
        {label}
      </p>
      {note && (
        <p className={fr.cx("fr-hint-text", "fr-mt-1w", "fr-mb-0")}>{note}</p>
      )}
    </div>
  );
}
