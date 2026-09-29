import {
  createGetHtmlAttributes,
  DsfrHeadBase,
  type DsfrHeadProps,
} from "@codegouvfr/react-dsfr/next-app-router/server-only-index";
import Link from "next/link";
import { defaultColorScheme } from "./defaultColorScheme";

export const { getHtmlAttributes } = createGetHtmlAttributes({
  defaultColorScheme,
});

export function DsfrHead(props: DsfrHeadProps) {
  return <DsfrHeadBase {...props} Link={Link} />;
}
