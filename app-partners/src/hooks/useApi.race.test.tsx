// @vitest-environment jsdom
import { useApi } from "@/hooks/useApi";
import { renderHook, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

afterEach(() => vi.unstubAllGlobals());

it("garde la réponse de la dernière URL quand une réponse plus ancienne arrive après", async () => {
  const pending: Record<string, (body: string) => void> = {};
  vi.stubGlobal(
    "fetch",
    vi.fn(
      (url: string) =>
        new Promise((resolve) => {
          pending[url] = (body) => resolve(new Response(body, { status: 200 }));
        }),
    ),
  );

  const { result, rerender } = renderHook(({ url }) => useApi<string[]>(url), { initialProps: { url: "/old" } });
  rerender({ url: "/new" });
  await waitFor(() => expect(pending["/new"]).toBeDefined());

  pending["/new"]('["nouvelle"]');
  await waitFor(() => expect(result.current.data).toEqual(["nouvelle"]));
  pending["/old"]('["périmée"]');
  await new Promise((r) => setTimeout(r, 20));

  expect(result.current.data).toEqual(["nouvelle"]);
  expect(result.current.loading).toBe(false);
});
