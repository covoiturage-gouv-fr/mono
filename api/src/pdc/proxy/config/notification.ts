import { env_or_fail, env_or_false } from "@/lib/env/index.ts";

export const mail = {
  smtp: {
    url: env_or_fail("APP_MAIL_SMTP_URL"),
  },
  debug: env_or_false("APP_MAIL_DEBUG_MODE"),
  verifySmtp: env_or_false("APP_MAIL_VERIFY_SMTP"),
  from: {
    name: env_or_fail("APP_MAIL_FROM_NAME", "Preuve de covoiturage"),
    email: env_or_fail(
      "APP_MAIL_FROM_EMAIL",
      "contact@covoiturage.beta.gouv.fr",
    ),
  },
  to: {
    name: env_or_fail("APP_MAIL_DEBUG_NAME", "Preuve de covoiturage"),
    email: env_or_fail(
      "APP_MAIL_DEBUG_EMAIL",
      "contact@covoiturage.beta.gouv.fr",
    ),
  },
};
