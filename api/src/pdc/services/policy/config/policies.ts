import { env_or_int } from "@/lib/env/index.ts";

export const finalize = {
  from: env_or_int("APP_POLICY_FINALIZE_DEFAULT_FROM", 15),
  to: env_or_int("APP_POLICY_FINALIZE_DEFAULT_TO", 2),
};
