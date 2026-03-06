import { assert, assertEquals, assertRejects, beforeAll, describe, it } from "@/dev_deps.ts";
import { handler, kernel as kernelDecorator, serviceProvider } from "@/ilos/common/Decorators.ts";
import { Exception } from "@/ilos/common/index.ts";
import { Action as AbstractAction, ServiceProvider as AbstractServiceProvider } from "@/ilos/core/index.ts";
import { Kernel as AbstractKernel } from "@/ilos/framework/index.ts";
import { ValidatorMiddleware } from "@/pdc/providers/superstruct/ValidatorMiddleware.ts";
import * as superstruct from "dep:superstruct";

describe("Validator Middleware v2", () => {
  const Test = superstruct.object({
    id: superstruct.number(),
    name: superstruct.string(),
  });

  @handler({
    service: "test",
    method: "testA",
    middlewares: [["validate", Test]],
  })
  class ActionA extends AbstractAction {
    protected override async handle(): Promise<string> {
      return "ok";
    }
  }

  @handler({
    service: "test",
    method: "testB",
    middlewares: [["validate", {}]],
  })
  class ActionB extends AbstractAction {
    protected override async handle(): Promise<string> {
      return "ok";
    }
  }

  @serviceProvider({
    middlewares: [[
      "validate",
      ValidatorMiddleware,
    ]],
    handlers: [
      ActionA,
      ActionB,
    ],
  })
  class ServiceProvider extends AbstractServiceProvider {}

  @kernelDecorator({
    children: [ServiceProvider],
  })
  class Kernel extends AbstractKernel {
  }

  const kernel = new Kernel();
  beforeAll(async () => {
    await kernel.bootstrap();
  });

  it("should pass if everything ok", async () => {
    const response = await kernel.call("test:testA", { id: 1, name: "toto" }, { channel: { service: "test" } });
    assert(response, "ok");
  });

  it("should raise error if validation error", async () => {
    const err = await assertRejects<Exception>(
      async () => await kernel.call("test:testA", { id: 1, name: 1 }, { channel: { service: "test" } }),
      Exception,
    );
    assertEquals(err.rpcError?.data, [
      "name : Expected a string, but received: 1",
    ]);
  });

  it("should raise error if validation middleware is not properly configured", async () => {
    const err = await assertRejects<Exception>(
      async () => await kernel.call("test:testB", { id: 1, name: "toto" }, { channel: { service: "test" } }),
      Exception,
    );
    assertEquals(err.rpcError?.data, "Validator has not been properly configured : {}");
  });
});
