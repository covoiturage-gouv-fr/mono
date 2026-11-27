import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { ContextType } from "@/ilos/common/index.ts";
import { SendWelcomeEmailParams } from "@/pdc/services/dashboard/interfaces/BrevoProviderInterface.ts";
import { CreateUserAction } from "./CreateUserAction.ts";

describe("CreateUserAction", () => {
  const appUrl = "https://partenaire.example.com";
  const config = { get: (k: string, d?: unknown) => (k === "app_url" ? appUrl : d) };
  const repository = {
    createUser: async () => ({ success: true, message: "ok", id: 7 }),
  };
  const operatorsRepository = { getOperatorById: async () => ({ siret: "12345678901234" }) };
  const territoriesRepository = { getTerritoryById: async () => null };

  const ctx: ContextType = {
    call: { user: { _id: 1, role: "operator.admin", permissions: [], operator_id: 3 } },
    channel: { service: "proxy" },
  };
  const params = { email: "u@example.com", firstname: "A", lastname: "B", role: "operator.user", operator_id: 3 };

  function makeAction(brevo: { sendWelcomeEmail: (p: SendWelcomeEmailParams) => Promise<void> }) {
    return new CreateUserAction(
      // deno-lint-ignore no-explicit-any
      repository as any,
      // deno-lint-ignore no-explicit-any
      operatorsRepository as any,
      // deno-lint-ignore no-explicit-any
      territoriesRepository as any,
      // deno-lint-ignore no-explicit-any
      brevo as any,
      // deno-lint-ignore no-explicit-any
      config as any,
    );
  }

  it("envoie l'URL du front comme login_url", async () => {
    const sent: SendWelcomeEmailParams[] = [];
    const action = makeAction({ sendWelcomeEmail: async (p) => void sent.push(p) });

    // deno-lint-ignore no-explicit-any
    await action.handle(params as any, ctx);

    assertEquals(sent, [{ email: "u@example.com", siret: "12345678901234", login_url: appUrl }]);
  });

  it("l'e-mail affiche le SIRET de connexion saisi quand il existe", async () => {
    const sent: SendWelcomeEmailParams[] = [];
    const action = makeAction({ sendWelcomeEmail: async (p) => void sent.push(p) });

    // deno-lint-ignore no-explicit-any
    await action.handle({ ...params, login_siret: "98765432100012" } as any, ctx);

    assertEquals(sent[0].siret, "98765432100012");
  });

  it("un échec Brevo ne fait pas échouer la création", async () => {
    const action = makeAction({ sendWelcomeEmail: () => Promise.reject(new Error("brevo down")) });

    // deno-lint-ignore no-explicit-any
    const result = await action.handle(params as any, ctx);

    assertEquals(result.id, 7);
  });
});
