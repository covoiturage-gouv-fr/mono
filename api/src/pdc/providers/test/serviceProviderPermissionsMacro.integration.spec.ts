import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import {
  ContextType,
  ForbiddenException,
  handler as handlerDecorator,
  InvalidParamsException,
  middleware,
  MiddlewareInterface,
  serviceProvider as serviceProviderDecorator,
} from "@/ilos/common/index.ts";
import {
  Action as AbstractAction,
  ServiceProvider as AbstractServiceProvider,
} from "@/ilos/core/index.ts";
import { get } from "@/lib/object/index.ts";
import {
  assertErrorHandler,
  assertSuccessHandler,
} from "@/pdc/providers/test/handlerMacro.ts";
import { makeKernelBeforeAfter } from "@/pdc/providers/test/helpers.ts";
import { KernelContext } from "@/pdc/providers/test/index.ts";

describe("Permission", () => {
  const handlerConfig = {
    service: "test",
    method: "run",
  } as const;
  type ParamsInterface = string | null;
  type ResultInterface = string | string[];

  @handlerDecorator({
    ...handlerConfig,
    middlewares: [["can", ["test.run"]]],
  })
  class Action extends AbstractAction {
    async handle(
      params: ParamsInterface,
      context: ContextType,
    ): Promise<ResultInterface> {
      return get(context, params || "", []);
    }
  }

  type ParamsType = {};
  type ResultType = {};

  @middleware()
  class PermissionMiddleware implements MiddlewareInterface {
    async process(
      params: ParamsType,
      context: ContextType,
      next: Function,
      neededPermissions: string[],
    ): Promise<ResultType> {
      if (!Array.isArray(neededPermissions) || neededPermissions.length === 0) {
        throw new InvalidParamsException("No permissions defined");
      }

      let permissions = [];

      if (
        !!context.call &&
        !!context.call.user &&
        !!context.call.user.permissions &&
        !!context.call.user.permissions.length
      ) {
        permissions = context.call.user.permissions;
      }

      if (permissions.length === 0) {
        throw new ForbiddenException("Invalid permissions");
      }

      const pass = neededPermissions.reduce(
        (p, c) => p && (permissions || []).indexOf(c) > -1,
        true,
      );

      if (!pass) {
        throw new ForbiddenException("Invalid permissions");
      }

      return next(params, context);
    }
  }

  /**
   * Mock the service provider
   */
  @serviceProviderDecorator({
    handlers: [Action],
    middlewares: [["can", PermissionMiddleware]],
  })
  class ServiceProvider extends AbstractServiceProvider {}

  const { before, after } = makeKernelBeforeAfter(
    ServiceProvider,
  );

  let context: KernelContext;
  beforeAll(async () => {
    context = await before();
  });

  afterAll(async () => {
    await after(context);
  });

  it("Success call.user.permissions", async () => {
    await assertSuccessHandler(
      context,
      handlerConfig,
      "call.user.permissions",
      ["test.run"],
      {
        call: { user: { permissions: ["test.run"] } },
        channel: { service: "dummy" },
      },
    );
  });

  it("Success channel.service", async () => {
    await assertSuccessHandler(
      context,
      handlerConfig,
      "channel.service",
      "dummy",
      {
        call: { user: { permissions: ["test.run"] } },
        channel: { service: "dummy" },
      },
    );
  });

  it("undefined permissions = Forbidden Error", async () => {
    await assertErrorHandler(
      context,
      handlerConfig,
      "call.user.permissions",
      "Forbidden Error",
    );
  });

  it("Error call.user.permissions", async () => {
    await assertErrorHandler(
      context,
      handlerConfig,
      "call.user.permissions",
      "Forbidden Error",
      {
        call: { user: { permissions: [] } },
        channel: { service: "dummy" },
      },
    );
  });
});
