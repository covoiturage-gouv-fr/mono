import { support } from "@/config/contacts.ts";
import { provider } from "@/ilos/common/Decorators.ts";
import { ContextType, KernelInterfaceResolver } from "@/ilos/common/index.ts";
import {
  MailTemplateNotificationInterface,
  NotificationTransporterInterfaceResolver,
} from "@/pdc/providers/notification/index.ts";
import { Export } from "@/pdc/services/export/models/Export.ts";
import { UserRepositoryInterfaceResolver } from "@/pdc/services/export/repositories/UserRepository.ts";
import { ExportCSVErrorNotification } from "../notifications/ExportCSVErrorNotification.ts";
import { ExportCSVNotification } from "../notifications/ExportCSVNotification.ts";
import { ExportCSVSupportNotification } from "../notifications/ExportCSVSupportNotification.ts";

export type NotificationProvider = {
  success(exp: Export): Promise<void>;
  error(exp: Export): Promise<void>;
  support(exp: Export): Promise<void>;
};

export abstract class NotificationProviderResolver implements NotificationProvider {
  public async success(_exp: Export): Promise<void> {
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
    protected userRepository: UserRepositoryInterfaceResolver,
    protected emailer: NotificationTransporterInterfaceResolver<MailTemplateNotificationInterface>,
  ) {}

  /**
   * Tell the export creator their export is ready (listed on the partner space)
   */
  public async success(exp: Export): Promise<void> {
    const { email, fullname } = await this.creator(exp);
    const notification = new ExportCSVNotification(
      `${fullname} <${email}>`,
      { fullname },
    );
    await this.emailer.send(notification);
  }

  /**
   * Send an error message to the export creator
   */
  public async error(exp: Export): Promise<void> {
    const { email, fullname } = await this.creator(exp);
    const notification = new ExportCSVErrorNotification(
      `${fullname} <${email}>`,
      { ...exp, error: exp.error },
    );
    await this.emailer.send(notification);
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

  protected async creator(exp: Export): Promise<{ email: string; fullname: string }> {
    const user = await this.userRepository.find(exp.created_by);
    if (!user) {
      throw new Error(`Export creator ${exp.created_by} not found`);
    }
    return { email: user.email, fullname: user.fullname };
  }
}
