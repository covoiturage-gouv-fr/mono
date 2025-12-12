import { support } from "@/config/contacts.ts";
import { provider } from "@/ilos/common/Decorators.ts";
import { ContextType, KernelInterfaceResolver } from "@/ilos/common/index.ts";
import {
  MailTemplateNotificationInterface,
  NotificationTransporterInterfaceResolver,
} from "@/pdc/providers/notification/index.ts";
import { Export } from "@/pdc/services/export/models/Export.ts";
import { ExportRecipient } from "@/pdc/services/export/models/ExportRecipient.ts";
import { ExportRepositoryInterfaceResolver } from "@/pdc/services/export/repositories/ExportRepository.ts";
import { ExportCSVErrorNotification } from "../notifications/ExportCSVErrorNotification.ts";
import { ExportCSVNotification } from "../notifications/ExportCSVNotification.ts";
import { ExportCSVSupportNotification } from "../notifications/ExportCSVSupportNotification.ts";

export type NotificationProvider = {
  success(exp: Export, url: string): Promise<void>;
  error(exp: Export): Promise<void>;
  support(exp: Export): Promise<void>;
};

export abstract class NotificationProviderResolver implements NotificationProvider {
  public async success(_exp: Export, _url: string): Promise<void> {
    throw new Error("Not implemented");
  }
  public async error(_exp: Export): Promise<void> {
    throw new Error("Not implemented");
  }
  public async support(_exp: Export): Promise<void> {
    throw new Error("Not implemented");
  }
}

@provider({
  identifier: NotificationProviderResolver,
})
export class NotificationService {
  protected defaultContext: ContextType = {
    channel: { service: "export" },
    call: { user: {} },
  };

  public constructor(
    protected kernel: KernelInterfaceResolver,
    protected exportRepository: ExportRepositoryInterfaceResolver,
    protected emailer: NotificationTransporterInterfaceResolver<MailTemplateNotificationInterface>,
  ) {}

  /**
   * Send the download link to the recipient
   */
  public async success(exp: Export, url: string): Promise<void> {
    const recipients = await this.recipients(exp);
    for (const { email, fullname } of recipients) {
      const notification = new ExportCSVNotification(
        `${fullname} <${email}>`,
        { fullname, action_href: url },
      );
      await this.emailer.send(notification);
    }
  }

  /**
   * Send an error message to the recipient
   */
  public async error(exp: Export): Promise<void> {
    const recipients = await this.recipients(exp);
    for (const { email, fullname } of recipients) {
      const notification = new ExportCSVErrorNotification(
        `${fullname} <${email}>`,
        { ...exp, error: exp.error },
      );
      await this.emailer.send(notification);
    }
  }

  /**
   * Notify the technical support about an error
   */
  public async support(exp: Export): Promise<void> {
    const { email, fullname } = support;
    const notification = new ExportCSVSupportNotification(
      `${fullname} <${email}>`,
      { ...exp, error: exp.error },
    );
    await this.emailer.send(notification);
  }

  protected async recipients(exp: Export): Promise<Pick<ExportRecipient, "email" | "fullname">[]> {
    const recipients = await this.exportRepository.recipients(exp._id);
    return recipients.map((recipient: ExportRecipient) => {
      return {
        email: recipient.email,
        fullname: recipient.fullname,
      };
    });
  }
}
