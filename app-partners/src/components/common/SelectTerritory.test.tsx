// @vitest-environment jsdom
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, describe, expect, test, vi } from "vitest";
import SelectTerritory from "./SelectTerritory";

const lyon = { _id: 7, name: "Métropole de Lyon", siret: "20004697700019" };

const respond = (rows: unknown[]) =>
  vi.spyOn(globalThis, "fetch").mockImplementation(() => {
    const body = {
      data: rows,
      meta: { pagination: { limit: 1, offset: 0, total: rows.length } },
    };
    return Promise.resolve(
      new Response(JSON.stringify(body), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );
  });

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("SelectTerritory", () => {
  test("affiche le libellé du territoire initial", async () => {
    const spy = respond([lyon]);
    render(<SelectTerritory defaultValue={7} onChange={vi.fn()} />);

    await waitFor(() =>
      expect(screen.getByRole<HTMLInputElement>("combobox").value).toContain(
        "Métropole de Lyon",
      ),
    );
    expect(
      spy.mock.calls.some(([url]) => (url as string).includes("id=7")),
    ).toBe(true);
  });

  test("renvoie l'identifiant et le nom du territoire choisi", async () => {
    respond([lyon]);
    const onChange = vi.fn();
    render(<SelectTerritory defaultValue={null} onChange={onChange} />);

    const input = screen.getByRole("combobox");
    fireEvent.focus(input);
    fireEvent.change(input, { target: { value: "lyon" } });
    fireEvent.click(await screen.findByRole("option", {}, { timeout: 2000 }));

    expect(onChange).toHaveBeenCalledWith(7, "Métropole de Lyon");
  });
});
