import { nextSearchQuery } from "@/hooks/useUrlSearch";
import { describe, expect, it } from "vitest";

describe("nextSearchQuery", () => {
  it("ne touche pas à l'URL quand le paramètre est déjà à jour", () => {
    expect(nextSearchQuery(new URLSearchParams(""), "q", "")).toBeNull();
    expect(nextSearchQuery(new URLSearchParams("q=abc"), "q", "abc")).toBeNull();
  });

  it("ajoute, change et retire le paramètre en gardant les autres", () => {
    expect(nextSearchQuery(new URLSearchParams("page=2"), "q", "abc")).toBe("page=2&q=abc");
    expect(nextSearchQuery(new URLSearchParams("q=a&page=2"), "q", "ab")).toBe("q=ab&page=2");
    expect(nextSearchQuery(new URLSearchParams("q=a&page=2"), "q", "")).toBe("page=2");
  });
});
