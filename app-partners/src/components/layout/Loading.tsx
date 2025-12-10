import Button from "@codegouvfr/react-dsfr/Button";

export default function Loading() {
  return (
    <p className="text-center">
      <Button iconId="ri-loader-2-fill" disabled priority="tertiary">
        Chargement ...
      </Button>
    </p>
  );
}
