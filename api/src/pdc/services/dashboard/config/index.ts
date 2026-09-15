import * as connections from "@/config/connections.ts";
import * as brevo from "./brevo.ts";
import * as notification from "./notification.ts";

export const config = {
  brevo,
  // SessionRepository (purge des sessions Redis) vit dans ce service : il lui faut connections.redis.
  connections,
  notification,
};
