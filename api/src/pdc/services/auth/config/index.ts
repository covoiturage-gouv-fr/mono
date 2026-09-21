import * as connections from "@/config/connections.ts";
import * as proxy from "@/config/proxy.ts";
import { env_or_fail } from "@/lib/env/index.ts";
import * as dex from "./dex.ts";
import * as permissions from "./permissions.ts";
import * as proconnect from "./proconnect.ts";
import * as test from "./test.ts";

export const env = env_or_fail("NODE_ENV", "local");
// Une seule URL de front, résolue dans `config/proxy.ts` (`APP_APP_URL`, ancien nom APP_DASHBOARD_V2_URL).
export const app_url = proxy.appUrl;
export const config = {
  app_url,
  connections,
  dex,
  env,
  permissions,
  proconnect,
  proxy,
  test,
};
