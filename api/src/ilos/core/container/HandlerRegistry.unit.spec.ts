import { assert } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { handler } from "@/ilos/common/index.ts";
import { Action } from "../foundation/Action.ts";
import { Container } from "../index.ts";
import { HandlerRegistry } from "./HandlerRegistry.ts";

describe("HandlerRegistry", () => {
  const defaultCallOptions = {
    method: "",
    params: null,
    context: null,
  };

  it("works", async () => {
    @handler({
      service: "hello",
      method: "world",
    })
    class HelloLocal extends Action {
      async call() {
        return "HelloLocal";
      }
    }

    @handler({
      service: "hello",
      method: "*",
    })
    class HelloLocalStar extends Action {
      async call() {
        return "HelloLocalStar";
      }
    }

    @handler({
      service: "hello",
      method: "world",
      local: false,
    })
    class HelloRemote extends Action {
      async call() {
        return "HelloRemote";
      }
    }

    @handler({
      service: "hello",
      method: "*",
      local: false,
    })
    class HelloRemoteStar extends Action {
      async call() {
        return "HelloRemoteStar";
      }
    }

    const container = new Container();
    const handlerRegistry = new HandlerRegistry(container);

    handlerRegistry.set(HelloLocal);
    handlerRegistry.set(HelloLocalStar);
    handlerRegistry.set(HelloRemote);
    handlerRegistry.set(HelloRemoteStar);

    const hr1 = handlerRegistry.get<null, null>({
      service: "hello",
      method: "world",
      local: true,
    });
    assert(hr1);
    assert(await hr1(defaultCallOptions) === "HelloLocal");

    const hr3 = handlerRegistry.get<null, null>({
      service: "hello",
      method: "*",
      local: true,
    });
    assert(hr3);
    assert(await hr3(defaultCallOptions) === "HelloLocalStar");

    const hr4 = handlerRegistry.get<null, null>({
      service: "hello",
      method: "notExisting",
      local: true,
    });
    assert(hr4);
    assert(await hr4(defaultCallOptions) === "HelloLocalStar");

    const hr5 = handlerRegistry.get<null, null>({
      service: "hello",
      method: "world",
      local: false,
    });
    assert(hr5);
    assert(await hr5(defaultCallOptions) === "HelloRemote");

    const hr6 = handlerRegistry.get<null, null>({
      service: "hello",
      method: "world",
      local: false,
    });
    assert(hr6);
    assert(await hr6(defaultCallOptions) === "HelloRemote");

    const hr7 = handlerRegistry.get<null, null>({
      service: "hello",
      method: "*",
      local: false,
    });
    assert(hr7);
    assert(await hr7(defaultCallOptions) === "HelloRemoteStar");

    const hr8 = handlerRegistry.get<null, null>({
      service: "hello",
      method: "notExisting",
      local: false,
    });
    assert(hr8);
    assert(await hr8(defaultCallOptions) === "HelloRemoteStar");
  });

  it("fallback to remote", async () => {
    @handler({
      service: "hello",
      method: "world",
      local: false,
    })
    class HelloRemote extends Action {
      async call() {
        return "HelloRemote";
      }
    }

    @handler({
      service: "hello",
      method: "*",
      local: false,
    })
    class HelloRemoteStar extends Action {
      async call() {
        return "HelloRemoteStar";
      }
    }

    const container = new Container();
    const handlerRegistry = new HandlerRegistry(container);

    handlerRegistry.set(HelloRemote);
    handlerRegistry.set(HelloRemoteStar);

    const hr1 = handlerRegistry.get<null, null>({
      service: "hello",
      method: "world",
      local: true,
    });
    assert(hr1);
    assert(await hr1(defaultCallOptions) === "HelloRemote");

    const hr2 = handlerRegistry.get<null, null>({
      service: "hello",
      method: "truc",
      local: true,
    });
    assert(hr2);
    assert(await hr2(defaultCallOptions) === "HelloRemoteStar");
  });

  it("fallback to sync", async () => {
    @handler({
      service: "hello",
      method: "world",
    })
    class HelloLocal extends Action {
      async call() {
        return "HelloLocal";
      }
    }

    @handler({
      service: "hello",
      method: "*",
      local: false,
    })
    class HelloRemoteStar extends Action {
      async call() {
        return "HelloRemoteStar";
      }
    }

    const container = new Container();
    const handlerRegistry = new HandlerRegistry(container);

    handlerRegistry.set(HelloLocal);
    handlerRegistry.set(HelloRemoteStar);

    const hr1 = handlerRegistry.get<null, null>({
      service: "hello",
      method: "world",
      local: true,
    });
    assert(hr1);
    assert(await hr1(defaultCallOptions) === "HelloLocal");

    const hr2 = handlerRegistry.get<null, null>({
      service: "hello",
      method: "truc",
      local: true,
    });
    assert(hr2);
    assert(await hr2(defaultCallOptions) === "HelloRemoteStar");

    const hr3 = handlerRegistry.get<null, null>({
      service: "hello",
      method: "world",
      local: false,
    });
    assert(hr3);
    assert(await hr3(defaultCallOptions) === "HelloRemoteStar");
  });
});
