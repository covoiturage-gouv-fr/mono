import { assertEquals, assertFalse } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { stub } from "dep:testing-mock";
import { UnauthorizedException } from "@/ilos/common/index.ts";
import { logger } from "@/lib/logger/index.ts";
import { errorHandlerMiddleware } from "./errorHandlerMiddleware.ts";

function callHandler(errMessage: string, errName = "Error") {
  const err = new Error(errMessage);
  err.name = errName;
  const req = { body: { id: 1, method: "test" } };
  let capturedStatus = 0;
  // deno-lint-ignore no-explicit-any
  let capturedBody: any = null;
  const res = {
    headersSent: false,
    status(code: number) {
      capturedStatus = code;
      return {
        json(data: unknown) {
          capturedBody = data;
        },
      };
    },
  };
  // deno-lint-ignore no-explicit-any
  errorHandlerMiddleware(err, req as any, res as any, (() => {}) as any);
  return { status: capturedStatus, body: capturedBody };
}

describe("errorHandlerMiddleware", () => {
  it("returns 400 for Bad Request Error", () => {
    const res = callHandler("Bad Request Error");
    assertEquals(res.status, 400);
  });

  it("returns 400 for Validation Error", () => {
    const res = callHandler("Validation Error");
    assertEquals(res.status, 400);
  });

  it("returns 401 for Unauthorized Error", () => {
    const res = callHandler("Unauthorized Error");
    assertEquals(res.status, 401);
  });

  it("returns 403 for Forbidden Error", () => {
    const res = callHandler("Forbidden Error");
    assertEquals(res.status, 403);
  });

  it("returns 404 for Not Found Error", () => {
    const res = callHandler("Not Found Error");
    assertEquals(res.status, 404);
  });

  it("returns 409 for Conflict Error", () => {
    const res = callHandler("Conflict Error");
    assertEquals(res.status, 409);
  });

  it("returns 429 for Too Many Requests Error", () => {
    const res = callHandler("Too Many Requests Error");
    assertEquals(res.status, 429);
  });

  it("returns 500 for Internal Server Error", () => {
    const res = callHandler("Internal Server Error");
    assertEquals(res.status, 500);
  });

  it("returns 500 for unknown errors", () => {
    const res = callHandler("Something unexpected happened", "TypeError");
    assertEquals(res.status, 500);
  });

  it("exposes error details for non-500 responses", () => {
    const res = callHandler("Bad Request Error", "ValidationError");
    const body = res.body as { error: { data: string; message: string } };
    assertEquals(body.error.data, "ValidationError");
    assertEquals(body.error.message, "Bad Request Error");
  });

  it("hides error details for 500 responses", () => {
    const res = callHandler("pg: relation 'secret_table' does not exist", "DatabaseError");
    const body = res.body as { error: { code: number; data: string; message: string } };
    assertEquals(body.error.code, 500);
    assertEquals(body.error.data, "Error");
    assertEquals(body.error.message, "Internal Server Error");
  });

  it("hides error details for explicit Internal Server Error", () => {
    const res = callHandler("Internal Server Error", "CustomInternalError");
    const body = res.body as { error: { data: string; message: string } };
    assertEquals(body.error.data, "Error");
    assertEquals(body.error.message, "Internal Server Error");
  });

  it("returns valid JSON-RPC 2.0 envelope", () => {
    const res = callHandler("Not Found Error");
    const body = res.body as { id: number; jsonrpc: string; error: { code: number } };
    assertEquals(body.id, 1);
    assertEquals(body.jsonrpc, "2.0");
    assertEquals(body.error.code, 404);
  });

  it("journalise au format [http] sans le message d'erreur", () => {
    using warn = stub(logger, "warn");
    const req = {
      method: "POST",
      originalUrl: "/v3.1/journeys?token=secret",
      baseUrl: "",
      route: { path: "/:api_version/journeys" },
      body: { email: "a@b.fr" },
    };
    const res = { headersSent: true };
    // deno-lint-ignore no-explicit-any
    errorHandlerMiddleware(new UnauthorizedException("token de jean@ex.fr"), req as any, res as any, (() => {}) as any);
    const line = warn.calls[0].args[0] as string;
    assertEquals(
      line,
      "[http] error status=401 error=UnauthorizedException method=POST route=/v3.1/journeys route_pattern=/:api_version/journeys",
    );
    assertFalse(line.includes("jean"));
    assertFalse(line.includes("secret"));
    assertEquals(req.body, { email: "a@b.fr" });
  });

  it("réserve le niveau error aux 5xx", () => {
    using error = stub(logger, "error");
    using warn = stub(logger, "warn");
    callHandler("Internal Server Error");
    callHandler("Not Found Error");
    assertEquals(error.calls.length, 1);
    assertEquals(warn.calls.length, 1);
  });
});
