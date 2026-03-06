import { assertEquals, assertObjectMatch } from "dep:assert";
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
import { assertNotEquals } from "https://deno.land/std@0.224.0/assert/assert_not_equals.ts";
import { Action } from "./Action.ts";
import { Kernel } from "./Kernel.ts";
import { ServiceProvider } from "./ServiceProvider.ts";

describe("Foundation: Kernel", () => {
  async function setup() {
    @provider()
    class Test implements ProviderInterface {
      protected base = "";
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
      service: "string",
      method: "hi",
    })
    class BasicAction extends Action {
      constructor(private test: Test) {
        super();
      }
      protected async handle(
        params: ParamsType,
        context: ContextType,
      ): Promise<ResultType> {
        if ("name" in params) {
          let from = "";
          if (context?.call?.user?.name) {
            from = ` from ${context.call.user.name}`;
          }
          return this.test.hello(`${params.name}${from}`);
        }
        throw new Error("Missing arguments");
      }
    }

    @handler({
      service: "math",
      method: "add",
    })
    class BasicTwoAction extends Action {
      constructor(private test: Test) {
        super();
      }
      protected async handle(
        params: ParamsType,
        context: ContextType,
      ): Promise<ResultType> {
        let count = 0;
        if ("add" in params) {
          const { add } = params;
          add.forEach((param: number) => {
            count += param;
          });
        } else {
          throw new Error("Please provide add param");
        }
        return this.test.hello(String(count));
      }
    }

    @serviceProvider({
      providers: [Test],
      handlers: [BasicTwoAction],
    })
    class BasicTwoServiceProvider extends ServiceProvider {}

    @serviceProvider({
      providers: [Test],
      children: [BasicTwoServiceProvider],
      handlers: [BasicAction],
    })
    class BasicServiceProvider extends ServiceProvider {}

    @serviceProvider({
      children: [BasicServiceProvider],
    })
    class BasicKernel extends Kernel {}

    const kernel = new BasicKernel();
    await kernel.bootstrap();

    return {
      kernel,
    };
  }

  it("should work with single call", async () => {
    const { kernel } = await setup();

    const response = await kernel.handle({
      jsonrpc: "2.0",
      id: 1,
      method: "math:add",
      params: {
        add: [1, 2],
      },
    });

    /**
     * response should not be 'void'
     * response should not be an array
     * response should match the expected object
     */
    assertNotEquals(response, undefined);
    assertNotEquals(Array.isArray(response), true);
    response && assertObjectMatch(response, {
      jsonrpc: "2.0",
      id: 1,
      result: "Hello 3",
    });
  });

  it("should work with a batch", async () => {
    const { kernel } = await setup();
    const response = await kernel.handle([
      {
        jsonrpc: "2.0",
        id: 1,
        method: "math:add",
        params: {
          add: [1, 2],
        },
      },
      {
        jsonrpc: "2.0",
        id: 2,
        method: "math:add",
        params: {
          add: [1, 5],
        },
      },
    ]);

    /**
     * response should not be 'void'
     * response should be an array
     * response should match the expected array
     */
    assertNotEquals(response, undefined);
    assertEquals(Array.isArray(response), true);
    response && assertEquals(response, [
      {
        jsonrpc: "2.0",
        id: 1,
        result: "Hello 3",
      },
      {
        jsonrpc: "2.0",
        id: 2,
        result: "Hi 6",
      },
    ]);
  });

  it("should return an error if service is unknown", async () => {
    const { kernel } = await setup();
    const response = await kernel.handle({
      jsonrpc: "2.0",
      id: 1,
      method: "nope:add",
      params: {
        add: [1, 2],
      },
    });

    /**
     * response should not be 'void'
     * response should not be an array
     * response should match the expected object
     */
    assertNotEquals(response, undefined);
    assertNotEquals(Array.isArray(response), true);
    response && assertObjectMatch(response, {
      jsonrpc: "2.0",
      id: 1,
      error: {
        code: -32601,
        data: "Handler not found for nope:add",
        message: "Method not found",
      },
    });
  });

  it("should return an error if method throws an error", async () => {
    const { kernel } = await setup();
    const response = await kernel.handle({
      jsonrpc: "2.0",
      id: 1,
      method: "math:add",
      params: {},
    });

    /**
     * response should not be 'void'
     * response should not be an array
     * response should match the expected object
     */
    assertNotEquals(response, undefined);
    assertNotEquals(Array.isArray(response), true);
    response && assertObjectMatch(response, {
      jsonrpc: "2.0",
      id: 1,
      error: {
        code: -32000,
        message: "Server error",
        data: "Please provide add param",
      },
    });
  });

  it("should work when call has context", async () => {
    const { kernel } = await setup();
    await kernel.bootstrap();
    const response = await kernel.handle({
      jsonrpc: "2.0",
      id: 1,
      method: "string:hi",
      params: {
        params: {
          name: "Jon",
        },
        _context: {
          channel: {
            service: "",
          },
          call: {
            user: {
              name: "Nicolas",
            },
          },
        },
      },
    });

    assertNotEquals(response, undefined);
    assertNotEquals(Array.isArray(response), true);
    response && assertObjectMatch(response, {
      jsonrpc: "2.0",
      id: 1,
      result: "Hello Jon from Nicolas",
    });
  });

  it("should work with notify call", async () => {
    const { kernel } = await setup();

    const response = await kernel.handle({
      jsonrpc: "2.0",
      method: "string:hi",
      params: {
        params: {
          name: "Jon",
        },
        _context: {
          channel: {
            service: "",
          },
          call: {
            user: {
              name: "Nicolas",
            },
          },
        },
      },
    });

    assertEquals(response, undefined);
  });
});
