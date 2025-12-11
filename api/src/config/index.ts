import { get } from "@/lib/object/index.ts";
import * as notifications from "./notifications.ts";

const fullConfiguration = {
  notifications,
};

export const config = {
  get<TValue>(path: string, defaultValue: TValue | undefined = undefined): TValue | null | undefined {
    return get<typeof fullConfiguration, TValue>(fullConfiguration, path, defaultValue);
  },
};
