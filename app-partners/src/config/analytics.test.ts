import { describe, expect, it } from "vitest";
import { matomoEnabled } from "./analytics";

const complete = { matomoUrl: "https://matomo.example.org", matomoSiteId: "7", matomoOrgDimensionId: "2" };

describe("matomoEnabled", () => {
  it("est actif avec les trois variables", () => {
    expect(matomoEnabled(complete)).toBe(true);
  });

  it.each(["matomoUrl", "matomoSiteId", "matomoOrgDimensionId"] as const)("est inactif sans %s", (key) => {
    expect(matomoEnabled({ ...complete, [key]: "" })).toBe(false);
  });

  it("est inactif pour des valeurs blanches", () => {
    expect(matomoEnabled({ matomoUrl: "  ", matomoSiteId: " ", matomoOrgDimensionId: " " })).toBe(false);
  });
});
