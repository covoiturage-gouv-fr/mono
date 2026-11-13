import * as connections from "@/config/connections.ts";
import { pickAppUrl } from "@/config/urls.ts";
import { env } from "@/lib/env/index.ts";
import * as dex from "@/pdc/services/auth/config/dex.ts";
import * as brevo from "./brevo.ts";
import * as notification from "./notification.ts";

export const config = {
  // URL du front (lien de connexion des e-mails). Pas via config/proxy.ts : il exige APP_SESSION_SECRET.
  app_url: pickAppUrl(env("APP_APP_URL"), env("APP_DASHBOARD_V2_URL")),
  brevo,
  // SessionRepository (purge des sessions Redis) vit dans ce service : il lui faut connections.redis.
  connections,
  // DeleteOperatorAction supprime les clés d'API de l'opérateur via DexClient.
  dex,
  notification,
};
