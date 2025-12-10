import { support } from "@/config/contacts.ts";
import { provider } from "@/ilos/common/Decorators.ts";
import { ContextType, KernelInterfaceResolver } from "@/ilos/common/index.ts";
import { Export } from "@/pdc/services/export/models/Export.ts";
import { ExportRecipient } from "@/pdc/services/export/models/ExportRecipient.ts";
import { ExportRepositoryInterfaceResolver } from "@/pdc/services/export/repositories/ExportRepository.ts";
import { ExportCSVSupportTemplateData } from "../notifications/ExportCSVSupportNotification.ts";

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
  ) {}

  /**
   * Send the download link to the recipient
   *
   * @param exp
   * @param url
   */
  public async success(exp: Export, url: string): Promise<void> {
    const recipients = await this.recipients(exp);
    for (const { email, fullname } of recipients) {
      await this.notify({
        template: "ExportCSVNotification",
        to: `${fullname} <${email}>`,
        data: { fullname, action_href: url },
      });
    }
  }

  /**
   * Send an error message to the recipient
   *
   * @param exp
   */
  public async error(exp: Export): Promise<void> {
    const recipients = await this.recipients(exp);
    for (const { email, fullname } of recipients) {
      await this.notify({
        template: "ExportCSVErrorNotification",
        to: `${fullname} <${email}>`,
        data: { fullname },
      });
    }
  }

  /**
   * Notify the technical support about an error
   *
   * @param exp
   */
  public async support(exp: Export): Promise<void> {
    const { email, fullname } = support;
    await this.notify<ExportCSVSupportTemplateData>({
      template: "ExportCSVSupportNotification",
      to: `${fullname} <${email}>`,
      data: { ...exp, error: exp.error },
    });
  }

  protected async recipients(
    exp: Export,
  ): Promise<Pick<ExportRecipient, "email" | "fullname">[]> {
    const recipients = await this.exportRepository.recipients(exp._id);
    return recipients.map((recipient: ExportRecipient) => {
      return {
        email: recipient.email,
        fullname: recipient.fullname,
      };
    });
  }

  protected async notify<T = unknown>(_payload: unknown): Promise<void> {
    // TODO migrate to provider

    // await this.kernel.call<NotifyParamsInterface<T>>(
    //   notifySignature,
    //   payload,
    //   this.defaultContext,
    // );
  }
}
