"use client";
import {
  DsfrProviderBase,
  type DsfrProviderProps,
  StartDsfrOnHydration,
} from "@codegouvfr/react-dsfr/next-app-router";
import { defaultColorScheme } from "./defaultColorScheme";
import Link from "next/link";

declare module "@codegouvfr/react-dsfr/next-app-router" {
  interface RegisterLink {
    Link: typeof Link;
  }
}

export function DsfrProvider(props: DsfrProviderProps) {
  return (
    <DsfrProviderBase
      defaultColorScheme={defaultColorScheme}
      Link={Link}
      {...props}
    />
  );
}

export { StartDsfrOnHydration };
