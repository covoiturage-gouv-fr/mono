import { env_or_fail, env_or_int } from "@/lib/env/index.ts";

export const apiUrl = env_or_fail("BREVO_API_URL", "https://api.brevo.com/v3/smtp/email");
export const apiKey = env_or_fail("BREVO_API_KEY", "");
export const welcomeTemplateId = env_or_int("BREVO_WELCOME_TEMPLATE_ID", 0);
