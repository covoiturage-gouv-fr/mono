import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { installNoDelay, withNoDelay } from "./noDelay.ts";

describe("withNoDelay", () => {
  it("active TCP_NODELAY sur une connexion TCP", async () => {
    const calls: boolean[] = [];
    const conn = { setNoDelay: (v: boolean) => calls.push(v) };
    const connect = withNoDelay((() => Promise.resolve(conn)) as unknown as typeof Deno.connect);

    const result = await connect({ hostname: "localhost", port: 5432 });

    assertEquals(result, conn as unknown as Deno.TcpConn);
    assertEquals(calls, [true]);
  });

  it("laisse passer une connexion sans setNoDelay (socket unix)", async () => {
    const conn = {};
    const connect = withNoDelay((() => Promise.resolve(conn)) as unknown as typeof Deno.connect);

    assertEquals(await connect({ hostname: "localhost", port: 5432 }), conn as unknown as Deno.TcpConn);
  });

  it("est installé une seule fois sur Deno.connect", () => {
    const before = Deno.connect;
    installNoDelay();
    assertEquals(Deno.connect, before);
  });
});
