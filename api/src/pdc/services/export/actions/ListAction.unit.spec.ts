import { assertEquals, assertThrows } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { ContextType, ForbiddenException } from "@/ilos/common/index.ts";
import { requireUserId } from "./ListAction.ts";

const ctx = (user: Record<string, unknown>): ContextType => ({
  call: { user },
  channel: { service: "proxy" },
});

describe("export: propriétaire obligatoire", () => {
  it("renvoie l'identifiant d'une session utilisateur", () => {
    assertEquals(requireUserId(ctx({ _id: 42, role: "territory.admin" })), 42);
  });

  // Jeton Bearer (credentials opérateur) : rôle et opérateur, mais pas d'identifiant utilisateur.
  // Sans garde, le filtre `created_by` disparaît et la liste renvoie tous les exports.
  it("refuse une session sans identifiant utilisateur", () => {
    assertThrows(
      () => requireUserId(ctx({ operator_id: 3, role: "operator.application" })),
      ForbiddenException,
    );
  });

  it("refuse un identifiant qui n'est pas un nombre", () => {
    assertThrows(() => requireUserId(ctx({ _id: "42" })), ForbiddenException);
    assertThrows(() => requireUserId(ctx({ _id: null })), ForbiddenException);
  });

  it("refuse un contexte sans utilisateur", () => {
    assertThrows(() => requireUserId({} as ContextType), ForbiddenException);
  });
});
