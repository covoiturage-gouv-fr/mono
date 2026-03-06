import { assertEquals, assertThrows } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { MethodNotFoundException } from "@/ilos/common/index.ts";
import {
  getConfigBySignature,
  getSignatureByConfig,
} from "./normalizeHandlerConfig.ts";

describe("Helpers: normalizeHandlerConfig", () => {
  it("normalize from string works", () => {
    const { method, service, version } = getConfigBySignature(
      "service@0.0.1:method",
    );
    assertEquals(method, "method");
    assertEquals(service, "service");
    assertEquals(version, "0.0.1");
  });

  it("normalize from string works with default version", () => {
    const { method, service, version } = getConfigBySignature("service:method");
    assertEquals(method, "method");
    assertEquals(service, "service");
    assertEquals(version, "latest");
  });

  it("normalize from string works raise error", () => {
    // Invalid method string (:method)
    assertThrows(
      () => getConfigBySignature(":method"),
      MethodNotFoundException,
    );
    // Invalid method string (service:)
    assertThrows(
      () => getConfigBySignature("service:"),
      MethodNotFoundException,
    );
    // Invalid method string (service:0.0.1)
    assertThrows(
      () => getConfigBySignature("service:0.0.1"),
      MethodNotFoundException,
    );
  });

  it("normalize from object works", () => {
    const method = getSignatureByConfig({
      service: "service",
      method: "method",
      version: "0.0.1",
    });
    assertEquals(method, "service@0.0.1:method");
  });

  it("normalize from object works with default version", () => {
    const method = getSignatureByConfig({
      service: "service",
      method: "method",
    });
    assertEquals(method, "service@latest:method");
  });

  it("normalize from object works raise error", () => {
    assertThrows(
      () => getSignatureByConfig({ service: "service", method: "" }),
      MethodNotFoundException,
    );
    assertThrows(
      () => getSignatureByConfig({ service: "", method: "method" }),
      MethodNotFoundException,
    );
  });
});
