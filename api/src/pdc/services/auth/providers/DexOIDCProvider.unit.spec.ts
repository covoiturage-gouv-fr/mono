import { assertEquals, assertInstanceOf, assertStrictEquals, assertThrows } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { UnauthorizedException } from "@/ilos/common/index.ts";
import { errors } from "dep:jose";
import { parseTokenSubject, toVerificationError } from "./DexOIDCProvider.ts";

const {
  JWKSNoMatchingKey,
  JWKSTimeout,
  JWSSignatureVerificationFailed,
  JWTClaimValidationFailed,
  JWTExpired,
  JWTInvalid,
} = errors;

describe("DexOIDCProvider.toVerificationError", () => {
  // Any token the client sends that we cannot positively verify is an auth
  // problem (401), never a server error (500).
  const tokenFailures = {
    "an expired token": new JWTExpired("token expired", {}),
    "a malformed token": new JWTInvalid("invalid"),
    "a bad signature": new JWSSignatureVerificationFailed(),
    "a failed claim (issuer/audience)": new JWTClaimValidationFailed("bad iss", {}),
    "an unknown signing key": new JWKSNoMatchingKey(),
  };

  for (const [label, error] of Object.entries(tokenFailures)) {
    it(`maps ${label} to a 401 UnauthorizedException`, () => {
      const out = toVerificationError(error);
      assertInstanceOf(out, UnauthorizedException);
      assertEquals(out.httpCode, 401);
    });
  }

  it("keeps a JWKS timeout untouched so it surfaces as a 500 (our infra is down)", () => {
    const original = new JWKSTimeout();
    assertStrictEquals(toVerificationError(original), original);
  });

  it("rethrows non-jose errors untouched", () => {
    const original = new TypeError("fetch failed");
    assertStrictEquals(toVerificationError(original), original);
  });
});

describe("DexOIDCProvider.parseTokenSubject", () => {
  it("parses a well-formed operator subject", () => {
    assertEquals(parseTokenSubject("operator.admin:1"), { role: "operator.admin", operator_id: 1 });
  });

  it("parses the legacy application role", () => {
    assertEquals(parseTokenSubject("application:42"), { role: "application", operator_id: 42 });
  });

  it("accepts operator_id 0 (e.g. registry.admin:0)", () => {
    assertEquals(parseTokenSubject("registry.admin:0"), { role: "registry.admin", operator_id: 0 });
  });

  const malformed = {
    "undefined": undefined,
    "empty": "",
    "missing operator_id": "operator.admin",
    "trailing colon / empty operator_id": "operator.admin:",
    "non-numeric operator_id": "operator.admin:abc",
    "leading colon / empty role": ":1",
    "extra segments": "operator.admin:1:2",
  };

  for (const [label, name] of Object.entries(malformed)) {
    it(`rejects a ${label} subject with 401`, () => {
      assertThrows(() => parseTokenSubject(name), UnauthorizedException);
    });
  }
});
