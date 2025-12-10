import { HandlebarsTemplateProvider } from "@/pdc/providers/template/index.ts";
import { NotificationMailTransporter } from "./NotificationMailTransporter.ts";

export { AbstractMailNotification } from "./AbstractNotification.ts";
export type * from "./interfaces/index.ts";
export { NotificationTransporterInterfaceResolver } from "./interfaces/index.ts";
export * from "./templates/DefaultNotification.ts";
export { NotificationMailTransporter };
export const defaultNotificationBindings = [
  HandlebarsTemplateProvider,
  NotificationMailTransporter,
];
