import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import {
  ContextType,
  handler as handlerDecorator,
  serviceProvider as serviceProviderDecorator,
} from "@/ilos/common/index.ts";
import {
  Action as AbstractAction,
  ServiceProvider as AbstractServiceProvider,
} from "@/ilos/core/index.ts";
import { makeKernelBeforeAfter } from "@/pdc/providers/test/helpers.ts";
import {
  assertErrorHandler,
  assertSuccessHandler,
  KernelContext,
} from "@/pdc/providers/test/index.ts";

describe("handler macro", () => {
  const handlerConfig = {
    service: "test",
    method: "hello",
  } as const;
  type ParamsInterface = string;
  type ResultInterface = string;

  class CustomError extends Error {
    constructor(message: string) {
      super(`custom:${message}`);
    }
  }

  @handlerDecorator(handlerConfig)
  class Action extends AbstractAction {
    async handle(
      params: ParamsInterface,
      context: ContextType,
    ): Promise<ResultInterface> {
      if (params === "error") {
        throw new CustomError("test");
      }
      return `hello ${params}`;
    }
  }

  @serviceProviderDecorator({
    handlers: [Action],
  })
  class ServiceProvider extends AbstractServiceProvider {}

  const { before, after } = makeKernelBeforeAfter(ServiceProvider);
  let context: KernelContext;

  beforeAll(async () => {
    context = await before();
  });

  afterAll(async () => {
    await after(context);
  });
  it("Success", async () => {
    await assertSuccessHandler(context, handlerConfig, "toto", "hello toto");
  });

  it("Error", async () => {
    await assertErrorHandler(context, handlerConfig, "error", "custom:test");
  });
});
