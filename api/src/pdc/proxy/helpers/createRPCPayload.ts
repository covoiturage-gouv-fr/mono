import { ParamsType, RPCSingleCallType } from "@/ilos/common/index.ts";
import { UserInterface } from "../types/UserInterface.ts";
import { injectContext } from "./injectContext.ts";

export function createRPCPayload(
  method: string,
  params: ParamsType,
  user?: Partial<UserInterface>,
  metadata?: any,
): RPCSingleCallType {
  return injectContext(
    {
      jsonrpc: "2.0",
      id: 1,
      method,
      params,
    },
    user,
    metadata,
  );
}
