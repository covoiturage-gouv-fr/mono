import { ContextType } from "@/ilos/common/index.ts";
import { UserInterface } from "../types/UserInterface.ts";

export function castContext(
  user?: Partial<UserInterface>,
  metadata?: any,
): ContextType {
  const call = { user, metadata };

  return {
    call,
    channel: {
      service: "proxy",
      transport: "node:http",
    },
  };
}
