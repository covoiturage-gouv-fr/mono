import { DestroyHookInterface, InitHookInterface } from "@/ilos/common/index.ts";

export interface NotificationTransporterInterface<D, O = { [key: string]: any }>
  extends InitHookInterface, DestroyHookInterface {
  send(data: D, options?: O): Promise<void>;
}

export abstract class NotificationTransporterInterfaceResolver<
  D,
  O = { [key: string]: any },
> implements NotificationTransporterInterface<D, O> {
  async init(): Promise<void> {}

  async destroy(): Promise<void> {}

  abstract send(data: D, options?: O): Promise<void>;
}
