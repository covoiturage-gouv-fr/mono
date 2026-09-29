import { assert, assertEquals } from "dep:assert";
import { beforeEach, describe, it } from "dep:testing-bdd";
import { Extensions } from "@/ilos/core/index.ts";
import { HandlebarsTemplateProvider } from "@/pdc/providers/template/index.ts";
import { Mail } from "dep:nodemailer";
import sinon from "dep:sinon";
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

  it("should escape placeholder values in message_html only", async () => {
    const notification = new DefaultNotification("toto <toto@example.com>", {
      fullname: `<b>x</b> & "y"`,
      message_html: "<p>{{ fullname }}</p>",
      message_text: "{{ fullname }}",
    });

    await transporter.send(notification);
    const { text, html } = stub.sendMail.getCall(0).args[0];
    assert((html as string).includes("<p>&lt;b&gt;x&lt;/b&gt; &amp; &quot;y&quot;</p>"));
    assert(!(html as string).includes("<b>x</b>"));
    // the text template already escapes message_text: moustache must not escape it twice
    assert(!(text as string).includes("&amp;lt;"));
  });

  it("should render missing placeholder values as empty", async () => {
    const notification = new DefaultNotification("toto <toto@example.com>", {
      message_html: "<p>[{{ unknown }}]</p>",
      message_text: "[{{ unknown }}]",
    });

    await transporter.send(notification);
    const { text, html } = stub.sendMail.getCall(0).args[0];
    assert((html as string).includes("<p>[]</p>"));
    assert((text as string).includes("[]"));
    assert(!(text as string).includes("undefined"));
  });
});
