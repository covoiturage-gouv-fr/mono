import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import {
  handler,
  HandlerInterface,
  lib,
  provider,
} from "@/ilos/common/index.ts";
import { Container } from "../index.ts";

describe("Container", () => {
  it("should define a handler and call it using a generic lib()", async () => {
    @lib()
    class _Hello {
      public world = "!!";
    }

    @handler({
      service: "test",
      method: "hello",
    })
    class Test implements HandlerInterface {
      public readonly middlewares = [];
      constructor(public hello: _Hello) {}

      async call() {
        return this.hello.world;
      }
    }

    const container = new Container();
    const h = container.resolve(Test);
    assertEquals(h.hello.world, "!!");

    container.setHandler(Test);

    const hBis = await container.getHandler<null, null, string>({
      service: "test",
      method: "hello",
    })({
      method: "",
      params: null,
      context: null,
    });
    assertEquals(hBis, "!!");
  });

  it("should inject a provider", async () => {
    @lib()
    class _HelloLib {
      public world = "yeah";
    }

    @provider()
    class _Hello {
      public world = "!!";
      constructor(public helloLib: _HelloLib) {}
      boot() {
        this.world = this.helloLib.world;
      }
    }

    @handler({
      service: "test",
      method: "hello",
    })
    class Test implements HandlerInterface {
      public readonly middlewares = [];
      constructor(public hello: _Hello) {}

      async call() {
        return this.hello.world;
      }
    }

    const container = new Container();
    const h = container.resolve(Test);
    assertEquals(h.hello.world, "yeah");

    container.setHandler(Test);
    const hbis = await container.getHandler<null, null, string>({
      service: "test",
      method: "hello",
    })({
      method: "",
      params: null,
      context: null,
    });
    assertEquals(hbis, "yeah");
  });

  it("should work without a boot provider", async () => {
    @provider()
    class _Hello {
      public world = "yeah";
    }

    @handler({
      service: "test",
      method: "hello",
    })
    class Test implements HandlerInterface {
      public readonly middlewares = [];
      constructor(public hello: _Hello) {}

      async call() {
        return this.hello.world;
      }
    }

    const container = new Container();
    const h = container.resolve(Test);
    assertEquals(h.hello.world, "yeah");

    container.setHandler(Test);

    const hbis = await container.getHandler<null, null, string>({
      service: "test",
      method: "hello",
    })({
      method: "",
      params: null,
      context: null,
    });
    assertEquals(hbis, "yeah");
  });
});
