import { StaticTemplateInterface } from "@/pdc/providers/template/index.ts";
import { MailTemplateNotificationInterface } from "./interfaces/index.ts";

export abstract class AbstractMailNotification<Data = any> implements MailTemplateNotificationInterface<Data> {
  static readonly templateText: StaticTemplateInterface;
  static readonly templateMJML: StaticTemplateInterface;
  static readonly subject: string;

  constructor(
    public readonly to: string,
    public readonly data: Partial<Data>,
  ) {}
}
