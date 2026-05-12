import { Infer, object } from "@/lib/superstruct/index.ts";
import {
  TerritoryCode,
  TerritoryType,
} from "@/pdc/providers/superstruct/shared/index.ts";

export const LastRecord = object({
  type: TerritoryType,
  code: TerritoryCode,
});

export type LastRecord = Infer<typeof LastRecord>;
