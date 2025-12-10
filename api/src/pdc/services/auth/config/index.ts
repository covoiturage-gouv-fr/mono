import * as connections from "@/config/connections.ts";
import * as proxy from "@/config/proxy.ts";
import { env_or_fail } from "@/lib/env/index.ts";
import * as dex from "./dex.ts";
import * as jwt from "./jwt.ts";
import * as permissions from "./permissions.ts";
import * as proconnect from "./proconnect.ts";

export const env = env_or_fail("NODE_ENV", "local");
export const app_url = env_or_fail("APP_DASHBOARD_V2_URL");
export const config = {
  app_url,
  connections,
  dex,
  env,
  jwt,
  permissions,
  proconnect,
  proxy,
};
