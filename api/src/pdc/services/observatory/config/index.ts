import { env } from "@/lib/env/index.ts";

export const config = {
  observatory: {
    get publishedUntil() {
      return env("APP_OBSERVATORY_PUBLISHED_UNTIL");
    },
  },
};
