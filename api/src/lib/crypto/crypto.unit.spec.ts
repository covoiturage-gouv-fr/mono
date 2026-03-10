import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { sha256sum } from "@/lib/crypto/index.ts";
import { getTmpDir, remove, writeSync } from "@/lib/file/index.ts";

describe("crypto:sha256sum", () => {
  const str = "Hello World!";
  const sha = "7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069";

  it("should return the correct hash from ReadableStream", async () => {
    const bytes = new TextEncoder().encode(str);
    const stream = ReadableStream.from<Uint8Array>([bytes]);
    assertEquals(await sha256sum(stream), sha);
  });

  it("should return the correct hash from filepath", async () => {
    const path = getTmpDir() + "/hello.txt";
    writeSync(path, str, { create: true });
    assertEquals(await sha256sum(path), sha);
    await remove(path);
  });
});
