"use client";

import {
  DsfrProviderBase,
  StartDsfrOnHydration,
} from "@codegouvfr/react-dsfr/next-app-router";
import Link from "next/link";
import type { ReactNode } from "react";
import { defaultColorScheme } from "./defaultColorScheme";

declare module "@codegouvfr/react-dsfr/next-app-router" {
  interface RegisterLink {
    Link: typeof Link;
  }
}

export function DsfrProvider(props: { children: ReactNode; lang: string }) {
  return (
    <DsfrProviderBase
      lang={props.lang}
      Link={Link}
      defaultColorScheme={defaultColorScheme}
    >
      {props.children}
    </DsfrProviderBase>
  );
}

export { StartDsfrOnHydration };
