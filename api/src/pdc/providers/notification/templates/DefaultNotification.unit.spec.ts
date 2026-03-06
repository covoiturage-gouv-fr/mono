import { assert, assertEquals, beforeEach, describe, it } from "@/dev_deps.ts";
import { Extensions } from "@/ilos/core/index.ts";
import { HandlebarsTemplateProvider } from "@/pdc/providers/template/index.ts";
import { Mail } from "dep:nodemailer";
import sinon from "dev:sinon";
import {
  DefaultNotification,
  DefaultTemplateData,
  NotificationMailTransporter,
  StaticMailTemplateNotificationInterface,
} from "../index.ts";

describe("default notification", () => {
  let transporter: NotificationMailTransporter;
  let stub: sinon.SinonStubbedInstance<Mail>;

  beforeEach(async () => {
    class NotificationOverride extends NotificationMailTransporter {
      override async init() {
        super.setOptionsFromConfig();
        await super.createTransport(false);
      }
    }

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
          debug: false,
          smtp: {},
        },
      },
    };
    const configProvider = new Extensions.ConfigStore(config);
    const templateProvider = new HandlebarsTemplateProvider();
    templateProvider.init();
    transporter = new NotificationOverride(
      configProvider,
      templateProvider,
    );
    await transporter.init();
    stub = sinon.stub(transporter.transporter);
  });

  it("should work", async () => {
    const sendTo = "toto <toto@example.com>";
    const sendMessage = "Tout va bien.";
    const notification = new DefaultNotification(sendTo, { message_text: sendMessage });
    const notificationCtor = notification.constructor as StaticMailTemplateNotificationInterface;

    await transporter.send(notification);
    assert(stub.sendMail.calledOnce);
    const { text, html, subject, to } = stub.sendMail.getCall(0).args[0];
    assertEquals(subject, notificationCtor.subject);
    assertEquals(to, sendTo);
    assert((text as string).search(sendMessage) > -1);
    assert((html as string).search(sendMessage) > -1);

    assert((text as string).search("https://covoiturage.beta.gouv.fr") > -1);
    assert((html as string).search("https://covoiturage.beta.gouv.fr") > -1);

    assert((text as string).search("contact@covoiturage.beta.gouv.fr") > -1);
    assert((html as string).search("contact@covoiturage.beta.gouv.fr") > -1);
  });

  it("should work with overriding", async () => {
    class TestNotification extends DefaultNotification {
      constructor(to: string, data: Partial<DefaultTemplateData>) {
        super(to, {
          app_url: "https://dev.covoiturage.beta.gouv.fr",
          ...data,
        });
      }
    }
    const sendTo = "toto <toto@example.com>";
    const sendMessage = "Tout va bien.";
    const notification = new TestNotification(sendTo, { message_text: sendMessage });
    const notificationCtor = notification.constructor as StaticMailTemplateNotificationInterface;

    await transporter.send(notification);
    assert(stub.sendMail.calledOnce);
    const { text, html, subject, to } = stub.sendMail.getCall(0).args[0];
    assertEquals(subject, notificationCtor.subject);

    assertEquals(to, sendTo);

    assert((text as string).search(sendMessage) > -1);
    assert((html as string).search(sendMessage) > -1);

    assert((text as string).search("https://dev.covoiturage.beta.gouv.fr") > -1);
    assert((html as string).search("https://dev.covoiturage.beta.gouv.fr") > -1);

    assert((text as string).search("contact@covoiturage.beta.gouv.fr") > -1);
    assert((html as string).search("contact@covoiturage.beta.gouv.fr") > -1);
  });
});
