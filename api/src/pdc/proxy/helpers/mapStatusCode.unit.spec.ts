import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { mapStatusCode } from "./mapStatusCode.ts";

describe("mapStatusCode", () => {
  it("RPC/HTTP status codes mapping: regular -> 200", () => {
    assertEquals(
      mapStatusCode({
        id: 1,
        jsonrpc: "2.0",
        result: {},
      }),
      200,
    );
  });

  it("RPC/HTTP status codes mapping: notification -> 204", () => {
    assertEquals(mapStatusCode(), 204);
  });

  it("RPC/HTTP status codes mapping: Parse error -> 422", () => {
    assertEquals(
      mapStatusCode({
        id: 1,
        jsonrpc: "2.0",
        error: { code: -32700, message: "Parse error" },
      }),
      422,
    );
  });

  it("RPC/HTTP status codes mapping: Invalid Request -> 400", () => {
    assertEquals(
      mapStatusCode({
        id: 1,
        jsonrpc: "2.0",
        error: { code: -32600, message: "Invalid Request" },
      }),
      400,
    );
  });

  it("RPC/HTTP status codes mapping: Invalid Params -> 400", () => {
    assertEquals(
      mapStatusCode({
        id: 1,
        jsonrpc: "2.0",
        error: { code: -32602, message: "Invalid Params" },
      }),
      400,
    );
  });

  it("RPC/HTTP status codes mapping: Method not found -> 405", () => {
    assertEquals(
      mapStatusCode({
        id: 1,
        jsonrpc: "2.0",
        error: { code: -32601, message: "Method not found" },
      }),
      405,
    );
  });

  it("RPC/HTTP status codes mapping: Internal error -> 500", () => {
    assertEquals(
      mapStatusCode({
        id: 1,
        jsonrpc: "2.0",
        error: { code: -32603, message: "Internal error" },
      }),
      500,
    );
  });

  it("RPC/HTTP status codes mapping: Server error 32000 -> 500", () => {
    assertEquals(
      mapStatusCode({
        id: 1,
        jsonrpc: "2.0",
        error: { code: -32000, message: "Server error" },
      }),
      500,
    );
  });

  it("RPC/HTTP status codes mapping: Server error 32099 -> 500", () => {
    assertEquals(
      mapStatusCode({
        id: 1,
        jsonrpc: "2.0",
        error: { code: -32000, message: "Server error" },
      }),
      500,
    );
  });

  it("RPC/HTTP status codes mapping: Unauthorized -> 401", () => {
    assertEquals(
      mapStatusCode({
        id: 1,
        jsonrpc: "2.0",
        error: { code: -32501, message: "Unauthorized" },
      }),
      401,
    );
  });

  it("RPC/HTTP status codes mapping: Forbidden -> 403", () => {
    assertEquals(
      mapStatusCode({
        id: 1,
        jsonrpc: "2.0",
        error: { code: -32503, message: "Forbidden" },
      }),
      403,
    );
  });

  it("RPC/HTTP status codes mapping: Conflict -> 409", () => {
    assertEquals(
      mapStatusCode({
        id: 1,
        jsonrpc: "2.0",
        error: { code: -32509, message: "Conflict" },
      }),
      409,
    );
  });

  it("RPC/HTTP status codes mapping: handles array response", () => {
    assertEquals(
      mapStatusCode([{
        id: 1,
        error: { data: "email conflict", code: -32509, message: "Conflict" },
        jsonrpc: "2.0",
      }]),
      409,
    );
  });
});
