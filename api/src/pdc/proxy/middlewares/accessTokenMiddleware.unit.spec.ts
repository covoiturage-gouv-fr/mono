import { assert, assertEquals, assertInstanceOf, assertStrictEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { KernelInterface, UnauthorizedException } from "@/ilos/common/index.ts";
import { NextFunction, Request as ExpressRequest, Response } from "dep:express";
import { accessTokenMiddleware } from "./accessTokenMiddleware.ts";

type VerifyFn = (token: string) => Promise<unknown>;
type StatelessReq = ExpressRequest & {
  stateless?: { user?: { operator_id: number; role: string; email: string; permissions: unknown[] } };
};

// Minimal kernel stub: kernel.get(DexOIDCProvider) returns an object exposing verifyToken.
function makeKernel(verifyToken: VerifyFn): KernelInterface {
  return { get: () => ({ verifyToken }) } as unknown as KernelInterface;
}

function makeReq(authorization?: string): StatelessReq {
  return { headers: authorization ? { authorization } : {} } as unknown as StatelessReq;
}

// Run the middleware and capture what was passed to next().
async function run(kernel: KernelInterface, req: StatelessReq) {
  const sentinel = Symbol("not-called");
  let nextArg: unknown = sentinel;
  const next: NextFunction = (e?: unknown) => {
    nextArg = e;
  };
  await accessTokenMiddleware(kernel)(req, {} as unknown as Response, next);
  return nextArg === sentinel ? sentinel : nextArg;
}

describe("accessTokenMiddleware", () => {
  it("rejects with 401 when no Authorization header is present", async () => {
    const kernel = makeKernel(() => Promise.reject(new Error("verifyToken must not run")));
    const err = await run(kernel, makeReq());
    assertInstanceOf(err, UnauthorizedException);
    assertEquals(err.httpCode, 401);
  });

  it("rejects with 401 when the Bearer token is empty", async () => {
    const kernel = makeKernel(() => Promise.reject(new Error("verifyToken must not run")));
    const err = await run(kernel, makeReq("Bearer "));
    assertInstanceOf(err, UnauthorizedException);
  });

  it("populates the stateless user on a valid token and continues", async () => {
    const req = makeReq("Bearer good");
    const kernel = makeKernel(() =>
      Promise.resolve({ operator_id: 7, role: "operator.admin", token_id: "ops@example.fr" })
    );
    const err = await run(kernel, req);
    assertEquals(err, undefined); // next() called with no error
    const user = req.stateless?.user;
    assertEquals(user?.operator_id, 7);
    assertEquals(user?.role, "operator.admin");
    assertEquals(user?.email, "ops@example.fr");
    assert(Array.isArray(user?.permissions) && user.permissions.length > 0);
  });

  it("forwards the auth error and never populates a user when verification fails", async () => {
    const req = makeReq("Bearer bad");
    const boom = new UnauthorizedException("nope");
    const kernel = makeKernel(() => Promise.reject(boom));
    const err = await run(kernel, req);
    assertStrictEquals(err, boom);
    // SECURITY: a verification failure must never silently downgrade to an anonymous request.
    assertEquals(req.stateless, undefined);
  });

  it("never swallows an unexpected error into an anonymous request", async () => {
    const req = makeReq("Bearer weird");
    const kernel = makeKernel(() => Promise.reject(new TypeError("infra down")));
    const err = await run(kernel, req);
    assertInstanceOf(err, TypeError);
    assertEquals(req.stateless, undefined);
  });

  it("forwards even a non-Error rejection instead of continuing anonymously", async () => {
    // Regression guard: the old wrapper had an `else next()` branch that dropped
    // any rejection that was neither an Error nor a string, letting the request
    // through as anonymous.
    const req = makeReq("Bearer weird");
    const weird = { code: "NOT_AN_ERROR" };
    const kernel = makeKernel(() => Promise.reject(weird));
    const err = await run(kernel, req);
    assertStrictEquals(err, weird);
    assertEquals(req.stateless, undefined);
  });
});
