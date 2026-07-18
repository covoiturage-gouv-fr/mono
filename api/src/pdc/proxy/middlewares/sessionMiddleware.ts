import { ConfigInterfaceResolver, KernelInterface } from "@/ilos/common/index.ts";
import { env_or_fail } from "@/lib/env/index.ts";
import RedisStore from "dep:connect-redis";
import { NextFunction, Request, Response } from "dep:express";
import expressSession from "dep:express-session";
import { Redis } from "dep:redis";

// Préfixe des clés de session dans Redis (connect-redis). Source unique partagée avec SessionRepository.
export const SESSION_KEY_PREFIX = "proxy:";

export function sessionMiddleware(kernel: KernelInterface) {
  const config = kernel.get(ConfigInterfaceResolver);
  const sessionSecret = config.get("proxy.session.secret");
  const sessionName = config.get("proxy.session.name");
  const redisConfig = config.get("connections.redis");
  const redis = new Redis(redisConfig);

  const middleware = expressSession({
    cookie: {
      path: "/",
      httpOnly: true,
      maxAge: config.get("proxy.session.maxAge"),
      // https everywhere but in local development
      secure: env_or_fail("APP_ENV", "local") !== "local",
      sameSite: "lax",
    },

    name: sessionName,
    secret: sessionSecret,
    resave: false,
    saveUninitialized: false,
    store: new RedisStore({ client: redis, prefix: SESSION_KEY_PREFIX }),
  });

  return (req: Request, res: Response, next: NextFunction) => {
    if (req.headers.authorization) {
      return next();
    }

    return middleware(req, res, next);
  };
}
