import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { Export, ExportStatus } from "../models/Export.ts";
import { NotificationService } from "./NotificationService.ts";

/**
 * NotificationService now emails the export CREATOR, resolved from the
 * UserRepository via `created_by` (the multi-recipient feature was removed).
 */
describe("NotificationService", () => {
  const creator = { _id: 42, email: "u@test.fr", fullname: "U Ser" };

  function makeService(sent: { to: string }[]) {
    const emailer = {
      init: async () => {},
      destroy: async () => {},
      send: async (notification: { to: string }) => {
        sent.push(notification);
      },
    };
    const userRepository = {
      find: async (_id: number) => (_id === creator._id ? creator : null),
    };
    return new NotificationService(
      {} as never,
      userRepository as never,
      emailer as never,
    );
  }

  function makeExport(): Export {
    return Export.fromJSON({
      _id: 1,
      uuid: "abc",
      target: "operator",
      status: ExportStatus.RUNNING,
      created_by: creator._id,
      params: {},
      error: "boom",
    });
  }

  it("emails the export creator on success", async () => {
    const sent: { to: string }[] = [];
    const service = makeService(sent);

    await service.success(makeExport());

    assertEquals(sent.length, 1);
    assertEquals(sent[0].to, "U Ser <u@test.fr>");
  });

  it("emails the export creator on error", async () => {
    const sent: { to: string }[] = [];
    const service = makeService(sent);

    await service.error(makeExport());

    assertEquals(sent.length, 1);
    assertEquals(sent[0].to, "U Ser <u@test.fr>");
  });
});
