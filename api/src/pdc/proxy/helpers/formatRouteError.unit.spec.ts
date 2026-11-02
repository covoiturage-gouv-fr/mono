import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { NotFoundException, UnexpectedException, UnprocessableRequestException } from "@/ilos/common/index.ts";
import { formatRouteError, violationMessages } from "./formatRouteError.ts";

describe("formatRouteError", () => {
  it("keeps typed 4xx message (plain)", () => {
    const { status, body } = formatRouteError(new NotFoundException("no such thing"), false);
    assertEquals(status, 404);
    assertEquals(body, "no such thing");
  });

  it("keeps typed 4xx rpcError (rpc)", () => {
    const e = new NotFoundException("no such thing");
    const { status, body } = formatRouteError(e, true);
    assertEquals(status, 404);
    assertEquals(body, { jsonrpc: "2.0", id: 1, error: e.rpcError });
  });

  it("masks raw Error as 500 (plain)", () => {
    const { status, body } = formatRouteError(new Error('relation "foo" does not exist'), false);
    assertEquals(status, 500);
    assertEquals(body, { error: "Internal Server Error" });
  });

  it("masks raw Error as 500 (rpc)", () => {
    const { status, body } = formatRouteError(new TypeError("Cannot read properties of undefined"), true);
    assertEquals(status, 500);
    assertEquals(body, {
      jsonrpc: "2.0",
      id: 1,
      error: { code: 500, data: "Error", message: "Internal Server Error" },
    });
  });

  it("masks UnexpectedException (httpCode 500) even with a custom message", () => {
    const { status, body } = formatRouteError(new UnexpectedException("pg: password authentication failed"), true);
    assertEquals(status, 500);
    assertEquals(body, {
      jsonrpc: "2.0",
      id: 1,
      error: { code: 500, data: "Error", message: "Internal Server Error" },
    });
  });

  it("handles non-Error throws", () => {
    const { status, body } = formatRouteError("boom", false);
    assertEquals(status, 500);
    assertEquals(body, { error: "Internal Server Error" });
  });
  it("relaie les messages de violation d'un 400 (liste de chaînes)", () => {
    const err = { httpCode: 400, message: "Invalid params", rpcError: { data: ["email : Expected a valid email"] } };
    assertEquals(formatRouteError(err, false), { status: 400, body: ["email : Expected a valid email"] });
  });

  // ajv rend des objets décrivant le DTO : chemins, mots-clés, contraintes, valeur reçue.
  it("ne relaie pas la structure du DTO produite par ajv", () => {
    const err = {
      httpCode: 400,
      message: "Invalid params",
      rpcError: { data: [{ instancePath: "/siret", keyword: "pattern", params: { pattern: "^[0-9]{14}$" } }] },
    };
    assertEquals(formatRouteError(err, false), { status: 400, body: { error: "Invalid params" } });
  });

  it("garde le message d'un 403 sans le détail du périmètre refusé", () => {
    const err = { httpCode: 403, message: "Interdit", rpcError: { data: { scope: "territory_id", value: 2 } } };
    assertEquals(formatRouteError(err, false), { status: 403, body: { error: "Interdit" } });
  });

  it("filtre aussi le champ data de la réponse JSON-RPC", () => {
    const err = {
      httpCode: 400,
      message: "Invalid params",
      rpcError: { code: -32602, message: "Invalid params", data: [{ instancePath: "/siret" }] },
    };
    assertEquals(formatRouteError(err, true), {
      status: 400,
      body: { jsonrpc: "2.0", id: 1, error: { code: -32602, message: "Invalid params", data: undefined } },
    });
  });

  it("relaie le motif de refus d'un trajet (422 JSON-RPC)", () => {
    const data = {
      terms_violation_labels: ["too_many_trips_by_day"],
      terms_violation_error_details: [
        { label: "too_many_trips_by_day", metas: { driver: 5, passenger: 1, limit: 4 } },
      ],
    };
    const err = new UnprocessableRequestException(data);
    assertEquals(formatRouteError(err, true), {
      status: 422,
      body: { jsonrpc: "2.0", id: 1, error: { code: -32422, message: "Unprocessable Request", data } },
    });
  });

  it("ne relaie que les champs du motif de refus", () => {
    const err = new UnprocessableRequestException({ terms_violation_labels: ["expired"], driver: { phone: "+33" } });
    assertEquals(formatRouteError(err, true).body, {
      jsonrpc: "2.0",
      id: 1,
      error: { code: -32422, message: "Unprocessable Request", data: { terms_violation_labels: ["expired"] } },
    });
  });

  it("ne relaie que label et les compteurs de metas dans les détails du refus", () => {
    const data = violationMessages({
      terms_violation_labels: ["too_many_trips_by_day"],
      terms_violation_error_details: [
        { label: "too_many_trips_by_day", metas: { driver: 5, passenger: 1, limit: 4, phone: "+33" }, phone: "+33" },
        { label: "expired", identity_key: "abc" },
        { metas: { driver: 1 } },
      ],
    });
    assertEquals(data, {
      terms_violation_labels: ["too_many_trips_by_day"],
      terms_violation_error_details: [
        { label: "too_many_trips_by_day", metas: { driver: 5, passenger: 1, limit: 4 } },
        { label: "expired" },
      ],
    });
  });

  it("violationMessages: chaîne, liste de chaînes, rien d'autre", () => {
    assertEquals(violationMessages("Start should be before end"), "Start should be before end");
    assertEquals(violationMessages(["a", "b"]), ["a", "b"]);
    assertEquals(violationMessages([]), undefined);
    assertEquals(violationMessages(["a", { b: 1 }]), undefined);
    assertEquals(violationMessages({ message: "x" }), undefined);
    assertEquals(violationMessages(undefined), undefined);
  });
});
