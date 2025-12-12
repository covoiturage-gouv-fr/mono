import { RPCSingleCallType } from "@/ilos/common/index.ts";
import { UserInterface } from "../types/UserInterface.ts";
import { castContext } from "./castContext.ts";

export function injectContext(
  doc: RPCSingleCallType,
  user?: Partial<UserInterface>,
  metadata?: any,
): RPCSingleCallType {
  return {
    id: doc.id,
    jsonrpc: doc.jsonrpc,
    method: doc.method,
    params: {
      params: doc.params,
      _context: castContext(user, metadata),
    },
  };
}
