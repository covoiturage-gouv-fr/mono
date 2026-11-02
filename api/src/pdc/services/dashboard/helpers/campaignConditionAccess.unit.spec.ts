import { assertEquals, assertThrows } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { ForbiddenException } from "@/ilos/common/index.ts";
import { assertCanCreate, assertCanWrite, callerOf, withTerritory } from "./campaignConditionAccess.ts";

describe("campaignConditionAccess", () => {
  const registry = callerOf({
    call: { user: { permissions: ["registry.campaign_condition.update"], operator_id: 3 } },
  });
  const operator3 = callerOf({
    call: { user: { permissions: ["operator.campaign_condition.update"], operator_id: 3 } },
  });
  const territory7 = withTerritory(
    callerOf({
      call: { user: { permissions: ["territory.campaign_condition.update"], operator_id: null, territory_id: 7 } },
    }),
    { territory_type: "aom", territory_code: "200046977", territory_label: "AOM 7" },
  );
  const onAom7 = { territory_type: "aom", territory_code: "200046977" };
  const elsewhere = { territory_type: "aom", territory_code: "200067999" };
  const mono = { ...elsewhere, operator_ids: [3], created_by_operator_id: 3, created_by_territory_id: null };
  const multi = { ...elsewhere, operator_ids: [3, 5], created_by_operator_id: null, created_by_territory_id: null };
  const byTerritory = { ...onAom7, operator_ids: [3, 5], created_by_operator_id: null, created_by_territory_id: 7 };
  const byOperatorOnAom7 = { ...onAom7, operator_ids: [3], created_by_operator_id: 3, created_by_territory_id: null };

  it("reconnaît l'équipe RPC par sa permission, même avec un operator_id en session", () => {
    assertEquals(registry, { registry: true, operator_id: 3, territory_id: null, territory: null });
    assertEquals(operator3, { registry: false, operator_id: 3, territory_id: null, territory: null });
    assertEquals(territory7.territory_id, 7);
  });

  it("ignore le territory_id d'un appelant opérateur", () => {
    const both = callerOf({ call: { user: { permissions: [], operator_id: 3, territory_id: 7 } } });
    assertEquals(both.territory_id, null);
  });

  it("laisse l'équipe RPC créer une campagne multi-opérateurs", () => {
    assertCanCreate([3, 5], registry);
  });

  it("laisse un territoire créer une campagne multi-opérateurs", () => {
    assertCanCreate([3, 5], territory7);
  });

  it("laisse un opérateur créer une campagne dont il est le seul opérateur", () => {
    assertCanCreate([3], operator3);
  });

  it("refuse à un opérateur une campagne multi-opérateurs ou celle d'un autre", () => {
    assertThrows(() => assertCanCreate([3, 5], operator3), ForbiddenException);
    assertThrows(() => assertCanCreate([5], operator3), ForbiddenException);
  });

  it("refuse un appelant sans opérateur, territoire ni permission RPC", () => {
    assertThrows(
      () => assertCanCreate([3], { registry: false, operator_id: null, territory_id: null, territory: null }),
      ForbiddenException,
    );
  });

  it("laisse un opérateur modifier sa campagne mono-opérateur sans la rendre multi", () => {
    assertCanWrite(mono, [3], operator3);
    assertThrows(() => assertCanWrite(mono, [3, 5], operator3), ForbiddenException);
  });

  it("refuse à un opérateur de modifier une campagne multi-opérateurs où il est listé", () => {
    assertThrows(() => assertCanWrite(multi, [3], operator3), ForbiddenException);
    assertThrows(() => assertCanWrite(byTerritory, [3], operator3), ForbiddenException);
  });

  it("refuse à un opérateur une campagne mono-opérateur créée par l'équipe RPC", () => {
    const byRegistry = { ...elsewhere, operator_ids: [3], created_by_operator_id: null, created_by_territory_id: null };
    assertThrows(() => assertCanWrite(byRegistry, [3], operator3), ForbiddenException);
  });

  it("laisse un territoire modifier les campagnes de son territoire, quel qu'en soit l'auteur", () => {
    assertCanWrite(byTerritory, [5], territory7);
    assertCanWrite(byOperatorOnAom7, [3, 5], territory7);
    assertThrows(() => assertCanWrite(multi, [3], territory7), ForbiddenException);
    assertThrows(() => assertCanWrite(mono, [3], territory7), ForbiddenException);
  });

  it("garde à un territoire sans équivalent datalake les seules campagnes qu'il a saisies", () => {
    const unresolved = withTerritory(
      callerOf({ call: { user: { permissions: [], operator_id: null, territory_id: 7 } } }),
      null,
    );
    assertCanWrite(byTerritory, [5], unresolved);
    assertThrows(() => assertCanWrite(byOperatorOnAom7, [3], unresolved), ForbiddenException);
  });

  it("laisse l'équipe RPC tout modifier", () => {
    assertCanWrite(multi, [5], registry);
  });
});
