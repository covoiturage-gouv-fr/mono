import { afterAll, assert, beforeAll, describe, it } from "@/dev_deps.ts";
import { Extensions } from "@/ilos/core/index.ts";
import { env_or_fail, env_or_false, env_or_int } from "@/lib/env/index.ts";
import { AbstractTemplate, HandlebarsTemplateProvider } from "@/pdc/providers/template/index.ts";
import { AbstractMailNotification } from "./AbstractNotification.ts";
import { NotificationMailTransporter } from "./NotificationMailTransporter.ts";

describe.skip("Notification", () => {
  const config = {
    notification: {
      mail: {
        from: {
          name: "admin",
          email: "admin@example.com",
        },
        to: {
          name: "test",
          email: "test@example.com",
        },
        debug: true,
        smtp: {
          host: env_or_fail("INTEGRATION_MAILER_SMTP_HOST", "mailer"),
          port: env_or_int("INTEGRATION_MAILER_SMTP_PORT", 1025),
          secure: env_or_false("INTEGRATION_MAILER_SMTP_SECURE"),
          auth: {
            user: env_or_fail(
              "INTEGRATION_MAILER_SMTP_USER",
              "test@example.com",
            ),
            pass: env_or_fail("INTEGRATION_MAILER_SMTP_PASS", "password"),
          },
        },
      },
    },
  };
  const configProvider = new Extensions.ConfigStore(config);
  const templateProvider = new HandlebarsTemplateProvider();
  let transporter: NotificationMailTransporter;

  beforeAll(async () => {
    templateProvider.init();
    transporter = new NotificationMailTransporter(
      configProvider,
      templateProvider,
    );
    await transporter.init();
  });

  afterAll(async () => {
    await transporter.destroy();
  });

  it("should work", async () => {
    class TemplateText extends AbstractTemplate<{ word: string }> {
      static template = "Hello {{word}}";
    }
    class TemplateMJML extends AbstractTemplate<{ word: string }> {
      static template = `       
            <mjml>
                <mj-body>
                    <mj-section>
                        <mj-column>Hello {{word}}'</mj-column>
                    </mj-section>
                </mj-body>
            </mjml>
        `;
    }
    class Notification extends AbstractMailNotification<{ word: string }> {
      static subject = "Test";
      static templateMJML = TemplateMJML;
      static templateText = TemplateText;
    }

    await transporter.send(
      new Notification("fake <fake@example.com>", { word: "world!" }),
    );
    assert(true);
  });
});
