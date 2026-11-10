import { describe, expect, it } from "vitest";
import { hasSimulatedSelection } from "./SimulationBanner";

describe("hasSimulatedSelection", () => {
  it("faux sans rôle simulé", () => {
    expect(hasSimulatedSelection({ operator_id: 1, territory_id: 2 })).toBe(false);
  });
  it("faux tant que l'id du rôle simulé est vide", () => {
    expect(hasSimulatedSelection({ simulatedRole: "operator", operator_id: null, territory_id: 2 })).toBe(false);
    expect(hasSimulatedSelection({ simulatedRole: "territory", territory_id: undefined })).toBe(false);
  });
  it("vrai dès que l'id du rôle simulé est posé", () => {
    expect(hasSimulatedSelection({ simulatedRole: "operator", operator_id: 1 })).toBe(true);
    expect(hasSimulatedSelection({ simulatedRole: "territory", territory_id: 2 })).toBe(true);
  });
});
