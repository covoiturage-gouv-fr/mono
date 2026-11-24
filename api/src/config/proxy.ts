import { env, env_or_fail, env_or_int } from "@/lib/env/index.ts";
import { getHostName } from "@/lib/net/index.ts";
import { taggedLogger } from "@/lib/logger/tagged.ts";
import { appUrlsDiverge, mergeOrigins, pickAppUrl } from "./urls.ts";

// `APP_DASHBOARD_V2_URL` est l'ancien nom de `APP_APP_URL` : à retirer des manifestes de déploiement.
if (appUrlsDiverge(env("APP_APP_URL"), env("APP_DASHBOARD_V2_URL"))) {
  taggedLogger("[boot]", { maxLength: 512 }).warn("app url diverge", { ignored: "APP_DASHBOARD_V2_URL" });
}

export const appUrl = pickAppUrl(env("APP_APP_URL"), env("APP_DASHBOARD_V2_URL"));
export const apiUrl = env_or_fail("APP_API_URL", "http://localhost:8080");
export const certUrl = env_or_fail("APP_CERT_URL", "http://localhost:4200");

export const port = env_or_int("PORT", 8080);
// Sondes Kubernetes, port non exposé par le Service.
export const healthPort = env_or_int("HEALTH_PORT", 8081);
export const hostname = getHostName();

export const session = {
  secret: env_or_fail("APP_SESSION_SECRET"),
  name: env_or_fail("APP_SESSION_NAME", "pdc-session"),

  /**
   * Cookie expiration (maxAge) in milliseconds
   * defaults to 30 days
   */
  maxAge: env_or_int("APP_SESSION_MAXAGE", 30 * 86400 * 1000),
};

export const rpc = {
  endpoint: env_or_fail("APP_RPC_ENDPOINT", "/rpc"),
};

// `APP_DASHBOARD_CORS` est un second nom pour la même liste : à retirer des manifestes.
export const cors = mergeOrigins(env("APP_CORS") || appUrl, env("APP_DASHBOARD_CORS") || appUrl);
