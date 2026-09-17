import { apiUrl } from "@/config/proxy.ts";
import { callbackUrl } from "@/config/urls.ts";
import { env, env_or_fail, env_or_false, env_or_true } from "@/lib/env/index.ts";

export const enabled = env_or_true("PROCONNECT_ENABLED");
export const require_mfa = env_or_false("PROCONNECT_REQUIRE_MFA");
export const client_id = env_or_fail("PROCONNECT_CLIENT_ID");
export const client_secret = env_or_fail("PROCONNECT_CLIENT_SECRET");
export const base_url = new URL(env_or_fail("PROCONNECT_BASE_URL"));

// Déduits de `APP_API_URL`, dont `AuthRouter` reconstruit l'URL de rappel : les deux doivent
// s'accorder, sinon le fournisseur refuse l'échange du code. Variables gardées en échappatoire.
export const redirect_url = env("PROCONNECT_REDIRECT_URL") || callbackUrl(apiUrl, "/auth/login/callback");
export const logout_redirect_url = env("PROCONNECT_LOGOUT_REDIRECT_URL") ||
  callbackUrl(apiUrl, "/auth/logout/callback");
