import * as dex from "@/pdc/services/auth/config/dex.ts";
import * as ajv from "./ajv.ts";

export const config = {
  ajv,
  // DeleteOperatorAction supprime les clés d'API de l'opérateur via DexClient.
  dex,
};
