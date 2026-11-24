import { assertEquals, assertRejects } from "dep:assert";
import { afterEach, describe, it } from "dep:testing-bdd";
import { Stub, stub } from "dep:testing-mock";
import { NotFoundException, UnexpectedException } from "@/ilos/common/index.ts";
import { Extensions } from "@/ilos/core/index.ts";

import { CompanyDataSourceProvider } from "./CompanyDataSourceProvider.ts";

const provider = (token = "key", timeout = 50) =>
  new CompanyDataSourceProvider(
    new Extensions.ConfigStore({ dataSource: { url: "https://insee.test", token, timeout } }),
  );

describe("CompanyDataSourceProvider (fetch simulé)", () => {
  let fetchStub: Stub | undefined;
  afterEach(() => fetchStub?.restore());

  it("échoue vite et explicitement quand l'API ne répond pas", async () => {
    fetchStub = stub(globalThis, "fetch", (_i, init) =>
      new Promise<Response>((_res, rej) => {
        init?.signal?.addEventListener("abort", () => rej(init.signal?.reason));
      }));
    const start = Date.now();
    const err = await assertRejects(() => provider().find("20003038500013"), UnexpectedException);
    assertEquals((err.rpcError?.data as string).includes("timeout"), true);
    assertEquals(Date.now() - start < 1000, true);
  });

  it("échoue explicitement sans clé d'API, sans appel réseau", async () => {
    fetchStub = stub(globalThis, "fetch", () => Promise.reject(new Error("unexpected call")));
    await assertRejects(() => provider("").find("20003038500013"), UnexpectedException);
    assertEquals(fetchStub.calls.length, 0);
  });

  it("404 -> NotFoundException", async () => {
    fetchStub = stub(globalThis, "fetch", () => Promise.resolve(new Response("{}", { status: 404 })));
    await assertRejects(() => provider().find("20003038500013"), NotFoundException);
  });
});
