import { assertEquals, assertRejects } from "dep:assert";
import { afterEach, describe, it } from "dep:testing-bdd";
import { ContextType, NotFoundException } from "@/ilos/common/index.ts";
import { ExternalCampaignsFeatureMiddleware } from "./ExternalCampaignsFeatureMiddleware.ts";

describe("ExternalCampaignsFeatureMiddleware", () => {
  const middleware = new ExternalCampaignsFeatureMiddleware();
  const next = async () => "next() called";
  const ctx = { call: {}, channel: { service: "proxy" } } as ContextType;
  const KEY = "APP_FEATURE_EXTERNAL_CAMPAIGNS";

  afterEach(() => Deno.env.delete(KEY));

  it("404 quand la variable est absente", async () => {
    Deno.env.delete(KEY);
    await assertRejects(() => middleware.process({}, ctx, next, undefined), NotFoundException);
  });

  it("404 quand la variable vaut autre chose que true", async () => {
    Deno.env.set(KEY, "false");
    await assertRejects(() => middleware.process({}, ctx, next, undefined), NotFoundException);
  });

  it("passe quand la variable vaut true", async () => {
    Deno.env.set(KEY, "true");
    assertEquals(await middleware.process({}, ctx, next, undefined), "next() called");
  });
});
