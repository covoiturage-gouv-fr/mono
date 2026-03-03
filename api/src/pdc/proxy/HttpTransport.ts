import bodyParser from "dep:body-parser";
import cors from "dep:cors";
import express, { NextFunction, Request, Response } from "dep:express";
import helmet from "dep:helmet";
import { Server } from "dep:http";
import { Redis } from "dep:redis";
import {
  children,
  ConfigInterface,
  ConfigInterfaceResolver,
  HandlerConfigType,
  InvalidRequestException,
  KernelInterface,
  proxy,
  RegisterHookInterface,
  router,
  RPCResponseType,
  RPCSingleCallType,
  TransportInterface,
  UnauthorizedException,
} from "@/ilos/common/index.ts";
import { handlerListIdentifier, ServiceProvider } from "@/ilos/core/index.ts";
import { env_or_fail, env_or_false } from "@/lib/env/index.ts";
import { logger } from "@/lib/logger/index.ts";
import { get } from "@/lib/object/index.ts";
import { join } from "@/lib/path/index.ts";
import { Sentry, SentryProvider } from "@/pdc/providers/sentry/index.ts";
import { registerExpressRoute } from "@/pdc/proxy/helpers/registerExpressRoute.ts";
import {
  ParamsInterface as GetAuthorizedCodesParams,
  ResultInterface as GetAuthorizedCodesResult,
  signature as getAuthorizedCodesSignature,
} from "@/pdc/services/territory/contracts/getAuthorizedCodes.contract.ts";
import { asyncHandler } from "./helpers/asyncHandler.ts";
import { createRPCPayload } from "./helpers/createRPCPayload.ts";
import { healthCheckFactory } from "./helpers/healthCheckFactory.ts";
import { injectContext } from "./helpers/injectContext.ts";
import { mapStatusCode } from "./helpers/mapStatusCode.ts";
import { prometheusMetricsFactory } from "./helpers/prometheusMetricsFactory.ts";
import { CacheMiddleware, cacheMiddleware } from "./middlewares/cacheMiddleware.ts";
import { dataWrapMiddleware, errorHandlerMiddleware } from "./middlewares/index.ts";
import { metricsMiddleware } from "./middlewares/metricsMiddleware.ts";
import {
  apiRateLimiter,
  contactformRateLimiter,
  monHonorCertificateRateLimiter,
  rateLimiter,
} from "./middlewares/rateLimiter.ts";
import { sessionMiddleware } from "./middlewares/sessionMiddleware.ts";

export class HttpTransport implements TransportInterface {
  app: express.Express;
  config: ConfigInterface;
  port: string;
  server: Server;
  cache: CacheMiddleware;

  constructor(private kernel: KernelInterface) {}

  getKernel(): KernelInterface {
    return this.kernel;
  }

  getInstance(): Server {
    return this.server;
  }

  async up(opts: string[] = []): Promise<void> {
    this.getProviders();

    const optsPort = parseInt(opts[0], 10);
    const port = optsPort || optsPort === 0 ? optsPort : this.config.get("proxy.port", 8080);

    this.app = express();
    this.setup();

    this.start(port);
  }

  async down(): Promise<void> {
    if (this.server) {
      this.server.close();
    }
  }

  setup(): void {
    this.registerBeforeAllHandlers();
    this.registerBodyHandler();
    this.registerSessionHandler();
    this.registerSecurity();
    this.registerMetrics();
    this.registerGlobalMiddlewares();
    this.registerCache();
    this.registerNestedRoutes();
    this.registerAcquisitionRoutes();
    this.registerSimulationRoutes();
    this.registerCeeRoutes();
    this.registerHonorRoutes();
    this.registerObservatoryRoutes();
    this.registerContactformRoute();
    this.registerCallHandler();
    this.registerAfterAllHandlers();
    this.registerGeoRoutes();
    this.registerStaticFolder();
  }

  getApp(): express.Express {
    return this.app;
  }

  private registerNestedRoutes() {
    this.kernel.getContainer().bind(proxy).toConstantValue(this.app);
    const serviceProviders = this.kernel.getContainer().getAll<ServiceProvider>(children);
    for (const serviceProvider of serviceProviders) {
      const container = serviceProvider.getContainer();
      if (container.isBound(router)) {
        const routerInstance = container.resolve<RegisterHookInterface>(container.get(router));
        routerInstance.register();
      }
    }
    const handlers = this.kernel.getContainer().getAll<HandlerConfigType>(handlerListIdentifier);
    for (const handler of handlers) {
      if (handler.apiRoute) {
        const config = {
          ...handler.apiRoute,
          action: `${handler.service}:${handler.method}`,
        };
        registerExpressRoute(this.app, this.kernel, config);
      }
    }
  }

  private async getProviders(): Promise<void> {
    this.config = this.kernel.getContainer().get(ConfigInterfaceResolver);
  }

  private registerBeforeAllHandlers(): void {
    this.kernel.getContainer().get(SentryProvider);
    Sentry.setTag("transport", "node:http");
    Sentry.setTag("version", this.config.get("sentry.version"));
  }

  private registerBodyHandler(): void {
    this.app.use(bodyParser.json({ limit: "2mb" }));
    this.app.use(bodyParser.urlencoded({ extended: false }));
  }

  private registerSessionHandler(): void {
    // needed for reverse-proxy compatibility
    // must be set before configuring the session
    this.app.set("trust proxy", 1);

    // Initiate the session from the cookie
    this.app.use(sessionMiddleware(this.kernel));
  }

  private registerSecurity(): void {
    // protect with typical headers and enable cors
    this.app.use(helmet());

    // apply CORS to all routes except the following ones
    this.app.use(
      /\/((?!honor|contactform|policy\/simulate|v3\/observatory|geo\/search).)*/,
      cors({
        origin: [this.config.get("proxy.cors"), this.config.get("proxy.dashboardV2Cors")],
        optionsSuccessStatus: 200,
        // Allow-Access-Credentials lets XHR requests send Cookies to a different URL
        credentials: true,
      }),
    );

    this.app.use(
      /\/(v3\/observatory|geo\/search)/i,
      cors({
        origin: this.config.get("proxy.observatoryCors"),
        optionsSuccessStatus: 200,
      }),
    );

    // apply specific cors policy
    this.app.use(
      "/honor",
      cors({
        origin: this.config.get("proxy.certUrl"),
        optionsSuccessStatus: 200,
      }),
    );
    this.app.use(
      "/contactform",
      cors({
        origin: this.config.get("proxy.showcase"),
        optionsSuccessStatus: 200,
      }),
    );
    this.app.use(
      "/geo/search",
      cors({
        origin: this.config.get("proxy.showcase"),
        optionsSuccessStatus: 200,
      }),
    );
    this.app.use(
      "/policy/simulate",
      cors({
        origin: this.config.get("proxy.showcase"),
        optionsSuccessStatus: 200,
      }),
    );
  }

  private registerGlobalMiddlewares(): void {
    // maintenance mode
    this.app.use((_req: Request, res: Response, next: NextFunction) => {
      if (env_or_false("APP_MAINTENANCE")) {
        res.status(503).json({ code: 503, error: "Service Unavailable" });
        return;
      }

      next();
    });

    // FIXME : expressMung.jsonAsync makes the response body undefined
    // this.app.use(signResponseMiddleware);
    this.app.use(dataWrapMiddleware);
  }

  private registerCache(): void {
    // create Redis connection only when the cache is enabled
    const enabled = this.config.get("cache.enabled");
    const gzipped = this.config.get("cache.gzipped");
    const authToken = this.config.get("cache.authToken");
    const driver = enabled ? new Redis(this.config.get("connections.redis")) : null;
    this.cache = cacheMiddleware({ enabled, driver, gzipped, authToken });

    this.app.delete(
      "/cache",
      rateLimiter(),
      this.cache.auth(),
      asyncHandler(async (req: Request, res: Response) => {
        const prefix = (req.query.prefix as string | undefined) || "*";
        const result = await this.cache.flush(prefix);
        res.status(200).json({ id: 1, jsonrpc: "2.0", result });
      }),
    );
  }

  private registerMetrics(): void {
    this.app.get(
      "/health",
      metricsMiddleware("health"),
      healthCheckFactory([]),
    );
    this.app.get(
      "/metrics",
      metricsMiddleware("metrics"),
      prometheusMetricsFactory(),
    );
  }

  private registerCeeRoutes(): void {
    // Routes have been migrated to apiRoute annotations in the action handlers
  }

  private registerSimulationRoutes(): void {
    // Routes have been migrated to apiRoute annotations in the action handlers
    this.app.post(
      "/policy/simulate",
      rateLimiter({ max: 1 }, "rl-policy-simulate"),
      asyncHandler(async (req: Request, res: Response, _next: NextFunction) => {
        // TODO : Should be queued
        await this.kernel.call("user:sendSimulationEmail", req.body, {
          call: { user: { permissions: ["common.user.policySimulate"] } },
          channel: {
            service: "proxy",
          },
        });
        this.send(res, { id: 1, jsonrpc: "2.0" });
      }),
    );
  }

  private registerGeoRoutes(): void {
    this.app.post(
      "/geo/search",
      rateLimiter(),
      asyncHandler(async (req: Request, res: Response, _next: NextFunction) => {
        const response = await this.kernel.handle(
          createRPCPayload(
            "territory:listGeo",
            { search: req.body.search, exclude_coms: req.body.exclude_coms },
            { permissions: ["common.territory.list"] },
          ),
        );
        this.send(res, response as RPCResponseType);
      }),
    );

    // Routes have been migrated to apiRoute annotations in the action handlers
  }

  /**
   * Journeys routes
   * - check status
   * - invalidate
   * - save
   */
  private registerAcquisitionRoutes(): void {
    // Routes have been migrated to apiRoute annotations in the action handlers
  }

  private registerAfterAllHandlers(): void {
    // add the RPC method as tag
    this.app.use(
      (error: Error, req: Request, _res: Response, next: NextFunction) => {
        const body = Array.isArray(req.body) ? req.body[0] : req.body;
        if (body) Sentry.setTag("method", get(body, "method", "not set"));

        next(error);
      },
    );

    Sentry.setupExpressErrorHandler(this.app);

    // general error handler
    // keep last
    this.app.use(errorHandlerMiddleware);
  }

  private registerHonorRoutes() {
    /**
     * Save a hit to the generated certificate (public or private sector)
     * 201 Response has a 'Location' header redirecting to the public stats page.
     * https://tools.ietf.org/html/rfc7231#section-6.3.2
     *
     * Rate limiter is pretty low as few certificates should be generated by IP per minute.
     * CORS policy is specific to this route
     */
    this.app.post(
      "/honor",
      monHonorCertificateRateLimiter(),
      asyncHandler(async (req: Request, res: Response, _next: NextFunction) => {
        await this.kernel.handle(
          createRPCPayload(
            "honor:save",
            { type: req.body.type, employer: req.body.employer },
            { permissions: ["common.honor.save"] },
          ),
        );
        res.status(201).header(
          "Location",
          `${env_or_fail("APP_APP_URL")}/honor/stats`,
        ).json({ saved: true });
      }),
    );

    /**
     * Expose stats publicly
     */
    this.app.get(
      "/honor/stats",
      rateLimiter(),
      asyncHandler(
        async (_req: Request, res: Response, _next: NextFunction) => {
          const response = await this.kernel.handle(
            createRPCPayload("honor:stats", {}, {
              permissions: ["common.honor.stats"],
            }),
          );
          this.send(res, response as RPCResponseType);
        },
      ),
    );
  }

  private registerObservatoryRoutes() {
    // Routes have been migrated to apiRoute annotations in the action handlers
  }

  /**
   * Used by showcase website's contact form.
   * Gets data and sends it by email
   */
  private registerContactformRoute() {
    this.app.post(
      "/contactform",
      contactformRateLimiter(),
      asyncHandler(async (req: Request, res: Response, _next: NextFunction) => {
        const { name, email, company, job, subject, body } = req.body;
        const response = await this.kernel.handle(
          createRPCPayload(
            "user:contactform",
            { name, email, company, job, subject, body },
            { permissions: ["common.user.contactform"] },
          ),
        );

        this.send(res, response as RPCResponseType);
      }),
    );
  }

  /**
   * Serve static files
   * Files must be copied to dist/public folder
   */
  private registerStaticFolder() {
    const __dirname = new URL(".", import.meta.url).pathname;
    logger.debug(
      `registerStaticFolder: ${join(__dirname, "public/.well-known")}`,
    );
    this.app.use(
      "/.well-known",
      express.static(join(__dirname, "public/.well-known")),
    );
  }

  /**
   * Calls to the /rpc endpoint
   */
  private registerCallHandler(): void {
    const endpoint = this.config.get("proxy.rpc.endpoint");

    /**
     * List all RPC actions
     * - disabled when deployed
     */
    if (env_or_fail("APP_ENV", "local") === "local") {
      this.app.get(
        endpoint,
        rateLimiter(),
        asyncHandler(
          async (_req: Request, res: Response, _next: NextFunction) => {
            const response = await this.kernel
              .getContainer()
              .getHandlers()
              .map((def) => ({
                service: def.service,
                method: def.method,
              }))
              .reduce((acc: string[], { service, method }) => {
                acc.push(`${service}:${method}`);
                return acc;
              }, []);
            res.json(response);
          },
        ),
      );
    }

    // register the POST route to /rpc
    this.app.post(
      endpoint,
      apiRateLimiter(),
      asyncHandler(
        async (
          req: Request,
          res: Response,
          _next: NextFunction,
        ): Promise<void> => {
          // if (req.originalUrl === "/rpc?methods=trip:stats") {
          //   res.setTimeout(120000);
          // }
          // inject the req.session.user to context in the body
          const isBatch = Array.isArray(req.body);
          let user = get(req, "session.user", null);

          if (!user) {
            throw new UnauthorizedException();
          }

          user = { ...user, ...(await this.getTerritoryInfos(user)) };
          // nest the params and _context and inject the session user
          // from { id: 1, jsonrpc: '2.0', method: 'a:b' params: {} }
          // to { id: 1, jsonrpc: '2.0', method: 'a:b' params: { params: {}, _context: {} } }
          req.body = isBatch
            ? req.body.map((doc: RPCSingleCallType) => injectContext(doc, user))
            : injectContext(req.body, user);

          // pass the request to the kernel
          const response = (await this.kernel.handle(req.body)) as RPCResponseType;

          // send the response
          this.send(res, response);
        },
      ),
    );
  }

  private start(port = 8080, _timeout = 30000): void {
    this.server = this.app.listen(
      port,
      () =>
        logger.info(
          `Listening on port ${port}. Version ${this.config.get("sentry.version")}`,
        ),
    );
    // FIXME
    // this.server.setTimeout(_timeout);
  }

  /**
   * Send the response to the client
   * - set optional headers
   * - set the status code (converted from an RPC status code)
   * - set the body. Error patterns are parsed
   */
  public send(
    res: Response,
    response: RPCResponseType,
    headers: { [key: string]: string } = {},
    unnestResult = false,
  ): void {
    if ("success" in (response as Response)) {
      this.setHeaders(res, headers);
    }

    // get the HTTP status code from response meta or convert RPC code
    const status = get(
      response,
      "result.meta.httpStatus",
      mapStatusCode(response),
    );

    res.status(status).json(this.parseErrorData(response, unnestResult));
  }

  /**
   * add non-null headers on successful responses
   */
  private setHeaders(
    res: Response,
    headers: { [key: string]: string } = {},
  ): void {
    Object.keys(headers).forEach((k: string) => {
      if (typeof headers[k] === "string") {
        res.set(k, headers[k]);
      }
    });
  }

  /**
   * Parse JSON payloads passed to the error.data object
   * clean up data key to avoid leaks and reduce size
   */
  private parseErrorData(
    response: Response,
    unnestResult = false,
  ): RPCResponseType {
    if (!("error" in response) || !("data" in response.error)) {
      return unnestResult ? response.result : response;
    }
    if (typeof response.error.data !== "string") return response;

    try {
      const parsed = JSON.parse(response.error.data);
      const cleaned = (Array.isArray(parsed) ? parsed : [parsed]).map((d) => {
        delete d.data;
        return d;
      });

      response.error.data = cleaned;
    } catch {}

    return response;
  }

  /**
   * Fetch additional data for territories
   */
  private async getTerritoryInfos(user: any): Promise<any> {
    if (user.territory_id) {
      const dt = { com: [] } as { com: string[] };

      try {
        const authorizedCodes = await this.kernel.call<
          GetAuthorizedCodesParams,
          GetAuthorizedCodesResult
        >(
          getAuthorizedCodesSignature,
          { _id: user.territory_id },
          { call: { user }, channel: { service: "proxy" } },
        );

        dt.com = authorizedCodes.com || [];
      } catch (e) {}

      user.authorizedZoneCodes = { ...dt };

      return user;
    }

    return {};
  }
}
