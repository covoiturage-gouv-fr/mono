import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import {
  ContextType,
  handler,
  ParamsType,
  provider,
  ProviderInterface,
  ResultType,
  serviceProvider,
} from "@/ilos/common/index.ts";
import { Action } from "./Action.ts";
import { ServiceProvider as ParentServiceProvider } from "./ServiceProvider.ts";

describe("Foundation: ServiceProvider", () => {
  const defaultContext = { channel: { service: "" } };

  it("should register handler", async () => {
    @handler({
      service: "test",
      method: "add",
    })
    class BasicAction extends Action {
      protected async handle(
        params: ParamsType,
        _context: ContextType,
      ): Promise<ResultType> {
        let count = 0;
        if ("add" in params) {
          const { add } = params;
          add.forEach((param: number) => {
            count += param;
          });
        }
        return count;
      }
    }

    @serviceProvider({
      handlers: [BasicAction],
    })
    class BasicServiceProvider extends ParentServiceProvider {}

    const sp = new BasicServiceProvider();
    await sp.register();
    await sp.init();

    const container = sp.getContainer();
    const response = await container.getHandler({
      service: "test",
      method: "add",
    })({
      params: {},
      context: defaultContext,
      method: "",
    });
    assertEquals(response, 0);
  });

  it("should register handler with extension", async () => {
    abstract class TestResolver {
      hello(_name: string) {
        throw new Error();
      }
    }

    @provider()
    class Test implements ProviderInterface {
      protected base: string = "";
      boot() {
        this.base = "Hello";
      }
      hello(name: string) {
        return `${this.base} ${name}`;
      }
    }

    @handler({
      service: "test",
      method: "hi",
    })
    class BasicAction extends Action {
      constructor(private test: TestResolver) {
        super();
      }
      protected async handle(
        params: ParamsType,
        _context: ContextType,
      ): Promise<ResultType> {
        if ("name" in params) {
          return this.test.hello(params.name);
        }
        throw new Error("Missing arguments");
      }
    }

    @serviceProvider({
      providers: [[TestResolver, new Test()]],
      handlers: [BasicAction],
    })
    class BasicServiceProvider extends ParentServiceProvider {}

    const sp = new BasicServiceProvider();
    await sp.register();
    await sp.init();

    const container = sp.getContainer();
    const handlerInstance = container.getHandler({
      service: "test",
      method: "hi",
    });
    const response = await handlerInstance({
      method: "fake",
      params: { name: "Sam" },
      context: defaultContext,
    });
    assertEquals(response, " Sam");
  });

  it("should register handler with alias and nested service providers", async () => {
    abstract class TestResolver {
      hello(_name: string) {
        throw new Error();
      }
    }

    @provider({
      identifier: TestResolver,
    })
    class Test implements ProviderInterface {
      protected base: string = "";
      boot() {
        this.base = "Hello";
      }
      hello(name: string) {
        const response = `${this.base} ${name}`;
        this.base = "Hi";
        return response;
      }
    }

    @handler({
      service: "test",
      method: "hi",
    })
    class BasicAction extends Action {
      constructor(private test: TestResolver) {
        super();
      }
      protected async handle(
        params: ParamsType,
        _context: ContextType,
      ): Promise<ResultType> {
        if ("name" in params) {
          return this.test.hello(params.name);
        }
        throw new Error("Missing arguments");
      }
    }

    @handler({
      service: "test",
      method: "add",
    })
    class BasicTwoAction extends Action {
      constructor(private test: TestResolver) {
        super();
      }
      protected async handle(
        params: ParamsType,
        _context: ContextType,
      ): Promise<ResultType> {
        let count = 0;
        if ("add" in params) {
          const { add } = params;
          add.forEach((param: number) => {
            count += param;
          });
        }
        return this.test.hello(String(count));
      }
    }

    @serviceProvider({
      providers: [Test],
      handlers: [BasicTwoAction],
    })
    class BasicTwoServiceProvider extends ParentServiceProvider {}

    @serviceProvider({
      providers: [Test],
      children: [BasicTwoServiceProvider],
      handlers: [BasicAction],
    })
    class BasicServiceProvider extends ParentServiceProvider {}

    const sp = new BasicServiceProvider();
    await sp.register();
    await sp.init();

    const container = sp.getContainer();
    const handlerInstance = container.getHandler({
      service: "test",
      method: "hi",
    });
    const response = await handlerInstance({
      method: "fake",
      params: { name: "Sam" },
      context: defaultContext,
    });
    assertEquals(response, "Hello Sam");

    const handlerTwoInstance = container.getHandler({
      service: "test",
      method: "add",
    });
    const responseTwo = await handlerTwoInstance({
      method: "fake",
      params: { add: [21, 21] },
      context: defaultContext,
    });
    assertEquals(responseTwo, "Hello 42");

    const responseTwoBis = await handlerTwoInstance({
      method: "fake",
      params: { add: [21, 21] },
      context: defaultContext,
    });
    assertEquals(responseTwoBis, "Hi 42");
  });
});
