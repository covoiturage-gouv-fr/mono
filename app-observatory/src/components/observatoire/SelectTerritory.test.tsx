// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import SelectTerritory from "./SelectTerritory";

const push = vi.fn();
vi.mock("next/navigation", () => ({ useRouter: () => ({ push }) }));

// Le millésime du tableau de bord est une entrée du composant : on le pilote.
let year = 2026;
vi.mock("../../context/DashboardProvider", () => ({
  useDashboardContext: () => ({
    dashboard: { params: { code: "XXXXX", name: "France", type: "country", year } },
  }),
}));

const DEBOUNCE_MS = 300;

const lyon = {
  id: "69123_com",
  territory: "69123",
  l_territory: "Lyon",
  type: "com",
  year: 2026,
};

const courtry = {
  id: "77139_com",
  territory: "77139",
  l_territory: "Courtry",
  type: "com",
  year: 2026,
};

const respondWith = (rows: unknown[]) =>
  vi.spyOn(globalThis, "fetch").mockResolvedValue(
    new Response(JSON.stringify(rows), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    }),
  );

const typeSearch = (...values: string[]) => {
  const input = screen.getByRole("combobox");
  values.forEach((value) => fireEvent.change(input, { target: { value } }));
};

const searchCalls = (spy: ReturnType<typeof respondWith>) =>
  spy.mock.calls.map((c) => c[0] as string).filter((u) => u.includes("territories/search"));

beforeEach(() => {
  year = 2026;
  push.mockClear();
});
afterEach(() => {
  // Pas de `globals: true` dans la config vitest : le nettoyage RTL automatique
  // n'est pas branché, sans quoi les rendus s'empilent d'un test à l'autre.
  cleanup();
  vi.restoreAllMocks();
});

describe("temporisation", () => {
  test("ne lance qu'une requête pour une rafale de frappes", async () => {
    const fetchSpy = respondWith([lyon]);
    render(<SelectTerritory url="territoire" />);

    typeSearch("l", "ly", "lyo", "lyon");

    await waitFor(() => expect(searchCalls(fetchSpy)).toHaveLength(1));
    expect(searchCalls(fetchSpy)[0]).toContain("q=lyon");
  });

  test("ne lance rien avant la fin du délai", async () => {
    const fetchSpy = respondWith([lyon]);
    render(<SelectTerritory url="territoire" />);

    typeSearch("lyon");

    expect(searchCalls(fetchSpy)).toHaveLength(0);
    await waitFor(() => expect(searchCalls(fetchSpy)).toHaveLength(1));
  });
});

describe("champ vidé", () => {
  test("rend la main au territoire courant sans requête", async () => {
    const fetchSpy = respondWith([lyon]);
    render(<SelectTerritory url="territoire" />);
    typeSearch("lyon");
    await waitFor(() => expect(searchCalls(fetchSpy)).toHaveLength(1));

    // Espace seul : même branche que le champ vidé, mais MUI garde la liste
    // ouverte, ce qui permet de lire les options restaurées.
    typeSearch(" ");
    await new Promise((r) => setTimeout(r, DEBOUNCE_MS * 2));

    expect(searchCalls(fetchSpy)).toHaveLength(1);
    expect(screen.queryByText("France")).not.toBeNull();
    expect(screen.queryByText("Lyon")).toBeNull();
  });
});

describe("millésime", () => {
  test("cible l'année du tableau de bord sur un millésime passé", async () => {
    year = 2022;
    const fetchSpy = respondWith([lyon]);
    render(<SelectTerritory url="territoire" />);

    typeSearch("lyon");

    await waitFor(() => expect(searchCalls(fetchSpy)[0]).toContain("year=2022"));
  });

  test("laisse l'API servir le dernier millésime sur l'année courante", async () => {
    const fetchSpy = respondWith([lyon]);
    render(<SelectTerritory url="territoire" />);

    typeSearch("lyon");

    await waitFor(() => expect(searchCalls(fetchSpy)).toHaveLength(1));
    expect(searchCalls(fetchSpy)[0]).not.toContain("year=");
  });
});

describe("nettoyage", () => {
  // Le timer d'un `search` remplacé (changement d'année) ou d'un composant démonté
  // tirerait sur l'ancien millésime.
  test("n'émet plus rien après démontage", async () => {
    const fetchSpy = respondWith([lyon]);
    const { unmount } = render(<SelectTerritory url="territoire" />);

    typeSearch("lyon");
    unmount();
    await new Promise((r) => setTimeout(r, DEBOUNCE_MS * 2));

    expect(searchCalls(fetchSpy)).toHaveLength(0);
  });
});

describe("sélection", () => {
  test("affiche les territoires renvoyés par l'API", async () => {
    respondWith([lyon]);
    render(<SelectTerritory url="territoire" />);

    typeSearch("lyon");

    expect(await screen.findByText("Lyon")).toBeDefined();
  });

  test("navigue vers le territoire choisi", async () => {
    respondWith([lyon]);
    render(<SelectTerritory url="territoire" />);
    typeSearch("lyon");

    fireEvent.click(await screen.findByText("Lyon"));

    expect(push).toHaveBeenCalledExactlyOnceWith(
      "/observatoire/territoire?code=69123&type=com",
    );
  });

  // Deux recherches espacées de 300 ms peuvent revenir dans le désordre : la
  // réponse lente de la frappe précédente ne doit pas écraser la plus récente.
  test("ignore la réponse d'une recherche remplacée", async () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch");
    fetchSpy.mockImplementationOnce(
      () =>
        new Promise((resolve) =>
          setTimeout(
            () => resolve(new Response(JSON.stringify([courtry]), { status: 200 })),
            DEBOUNCE_MS * 2,
          ),
        ),
    );
    fetchSpy.mockResolvedValue(new Response(JSON.stringify([lyon]), { status: 200 }));
    render(<SelectTerritory url="territoire" />);

    typeSearch("lyo");
    await waitFor(() => expect(searchCalls(fetchSpy)).toHaveLength(1));
    typeSearch("lyon");
    await waitFor(() => expect(searchCalls(fetchSpy)).toHaveLength(2));
    await new Promise((r) => setTimeout(r, DEBOUNCE_MS * 3));

    expect(screen.queryByText("Courtry")).toBeNull();
    expect(screen.queryByText("Lyon")).not.toBeNull();
  });

  test("annule la requête en vol quand la saisie change", async () => {
    const fetchSpy = respondWith([lyon]);
    render(<SelectTerritory url="territoire" />);

    typeSearch("lyo");
    await waitFor(() => expect(searchCalls(fetchSpy)).toHaveLength(1));
    typeSearch("lyon");
    await waitFor(() => expect(searchCalls(fetchSpy)).toHaveLength(2));

    expect((fetchSpy.mock.calls[0][1]!).signal!.aborted).toBe(true);
    expect((fetchSpy.mock.calls[1][1]!).signal!.aborted).toBe(false);
  });

  // MUI réinjecte le libellé formaté dans le champ après sélection : sans le
  // filtre sur `reason`, ça repartait en requête.
  test("ne relance pas de recherche après la sélection", async () => {
    const fetchSpy = respondWith([lyon]);
    render(<SelectTerritory url="territoire" />);
    typeSearch("lyon");
    await waitFor(() => expect(searchCalls(fetchSpy)).toHaveLength(1));

    fireEvent.click(await screen.findByText("Lyon"));
    await new Promise((r) => setTimeout(r, DEBOUNCE_MS * 2));

    expect(searchCalls(fetchSpy)).toHaveLength(1);
  });
});
