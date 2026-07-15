import { assertEquals, assertRejects, assertThrows } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { ConfigInterfaceResolver } from "@/ilos/common/index.ts";
import { NotificationTransporterInterfaceResolver } from "@/pdc/providers/notification/index.ts";
import {
  InactiveUserToWarn,
  UsersRepositoryInterfaceResolver,
} from "@/pdc/services/dashboard/interfaces/UsersRepositoryInterface.ts";
import { assertNotificationConfigured, PurgeInactiveUsersCommand } from "./PurgeInactiveUsersCommand.ts";

function fakeConfig(smtp: unknown): ConfigInterfaceResolver {
  return { get: (_k: string, d?: unknown) => smtp ?? d } as unknown as ConfigInterfaceResolver;
}

describe("assertNotificationConfigured", () => {
  it("throws quand la config SMTP est absente", () => {
    assertThrows(() => assertNotificationConfigured(fakeConfig(null)));
  });
  it("ne throw pas quand la config SMTP est présente", () => {
    assertNotificationConfigured(fakeConfig({ host: "smtp.example.com" }));
  });
});

describe("PurgeInactiveUsersCommand.call", () => {
  function fakeRepository(
    toWarn: InactiveUserToWarn[],
    toDelete: Array<{ _id: number }>,
  ) {
    const marked: number[] = [];
    const deleted: number[] = [];
    const calls: string[] = [];
    const repository = {
      findUsersToWarn: (_inactivity: string) => {
        calls.push("findUsersToWarn");
        return Promise.resolve(toWarn);
      },
      markUserWarned: (id: number) => {
        calls.push("markUserWarned");
        marked.push(id);
        return Promise.resolve();
      },
      findUsersToDelete: (_grace: string) => {
        calls.push("findUsersToDelete");
        return Promise.resolve(toDelete);
      },
      deleteInactiveUsers: (_grace: string) => {
        calls.push("deleteInactiveUsers");
        toDelete.forEach((u) => deleted.push(u._id));
        return Promise.resolve(toDelete);
      },
      deleteUser: ({ id }: { id: number }) => {
        calls.push("deleteUser");
        deleted.push(id);
        return Promise.resolve();
      },
    };
    return { repository: repository as unknown as UsersRepositoryInterfaceResolver, marked, deleted, calls };
  }

  function fakeEmailer(failFor: string[] = []) {
    const sent: string[] = [];
    const calls: string[] = [];
    const emailer = {
      send: (n: { to: string }) => {
        calls.push("send");
        if (failFor.some((email) => n.to.includes(email))) {
          return Promise.reject(new Error(`envoi échoué pour ${n.to}`));
        }
        sent.push(n.to);
        return Promise.resolve();
      },
    };
    return {
      emailer: emailer as unknown as NotificationTransporterInterfaceResolver<{ to: string }>,
      sent,
      calls,
    };
  }

  const options = { inactivity: "12 months", grace: "7 days", appUrl: "https://partenaire.example.com", dryRun: false };

  it("avertit puis marque, puis supprime (ordre send -> mark -> delete)", async () => {
    const toWarn: InactiveUserToWarn[] = [
      { _id: 1, email: "u1@example.com", firstname: "A", lastname: "B" },
      { _id: 2, email: "u2@example.com", firstname: "C", lastname: "D" },
    ];
    const { repository, marked, deleted } = fakeRepository(toWarn, [{ _id: 42 }]);
    const { emailer, sent } = fakeEmailer();
    const command = new PurgeInactiveUsersCommand(fakeConfig({ host: "smtp.example.com" }), repository, emailer);

    await command.call(options);

    assertEquals(sent.length, 2);
    assertEquals(marked, [1, 2]);
    assertEquals(deleted, [42]);
  });

  it("isole l'échec d'envoi : l'utilisateur en échec n'est jamais marqué, la boucle continue", async () => {
    const toWarn: InactiveUserToWarn[] = [
      { _id: 1, email: "u1@example.com", firstname: null, lastname: null },
      { _id: 2, email: "u2@example.com", firstname: null, lastname: null },
    ];
    const { repository, marked } = fakeRepository(toWarn, []);
    const { emailer, sent } = fakeEmailer(["u1@example.com"]);
    const command = new PurgeInactiveUsersCommand(fakeConfig({ host: "smtp.example.com" }), repository, emailer);

    await command.call(options);

    assertEquals(sent, ["u2@example.com <u2@example.com>"]);
    assertEquals(marked, [2]);
  });

  it("dry-run n'envoie, ne marque, ni ne supprime rien (aperçu via findUsersToDelete, jamais deleteInactiveUsers)", async () => {
    const toWarn: InactiveUserToWarn[] = [
      { _id: 1, email: "u1@example.com", firstname: null, lastname: null },
    ];
    const { repository, marked, deleted, calls } = fakeRepository(toWarn, [{ _id: 42 }]);
    const { emailer, sent } = fakeEmailer();
    const command = new PurgeInactiveUsersCommand(fakeConfig({ host: "smtp.example.com" }), repository, emailer);

    await command.call({ ...options, dryRun: true });

    assertEquals(sent.length, 0);
    assertEquals(marked.length, 0);
    assertEquals(deleted.length, 0);
    assertEquals(calls.includes("findUsersToDelete"), true);
    assertEquals(calls.includes("deleteInactiveUsers"), false);
  });

  it("run réel : suppression atomique via deleteInactiveUsers (jamais SELECT+deleteUser, anti-course)", async () => {
    const { repository, deleted, calls } = fakeRepository([], [{ _id: 42 }, { _id: 43 }]);
    const { emailer } = fakeEmailer();
    const command = new PurgeInactiveUsersCommand(fakeConfig({ host: "smtp.example.com" }), repository, emailer);

    await command.call(options);

    assertEquals(deleted, [42, 43]);
    assertEquals(calls.includes("deleteInactiveUsers"), true);
    // pas de fenêtre SELECT->DELETE : ni findUsersToDelete ni deleteUser en run réel
    assertEquals(calls.includes("findUsersToDelete"), false);
    assertEquals(calls.includes("deleteUser"), false);
  });

  it("le garde-fou stoppe avant tout traitement quand SMTP absent", async () => {
    const { repository, calls: repoCalls } = fakeRepository(
      [{ _id: 1, email: "u1@example.com", firstname: null, lastname: null }],
      [{ _id: 42 }],
    );
    const { emailer, calls: emailerCalls } = fakeEmailer();
    const command = new PurgeInactiveUsersCommand(fakeConfig(null), repository, emailer);

    await assertRejects(() => command.call(options));

    assertEquals(repoCalls.length, 0);
    assertEquals(emailerCalls.length, 0);
  });
});
