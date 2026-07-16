import { env_or_fail, env_or_false } from "@/lib/env/index.ts";

// Config du transporter mail (NotificationMailTransporter) requise à l'init du service :
// sans elle, l'init du provider throw et fait planter le boot de l'API.
export const email = "technique@covoiturage.beta.gouv.fr";
export const fullname = "Équipe technique";

export const mail = {
  smtp: {
    url: env_or_fail("APP_MAIL_SMTP_URL"),
  },
  debug: env_or_false("APP_MAIL_DEBUG_MODE"),
  from: {
    name: env_or_fail("APP_MAIL_FROM_NAME", ""),
    email: env_or_fail("APP_MAIL_FROM_EMAIL", ""),
  },
  to: {
    name: env_or_fail("APP_MAIL_DEBUG_NAME", ""),
    email: env_or_fail("APP_MAIL_DEBUG_EMAIL", ""),
  },
};
