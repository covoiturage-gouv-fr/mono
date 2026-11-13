// @vitest-environment jsdom
import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import { latestMillesime } from "../helpers/lists";
import { useDashboard } from "./useDashboard";

// La résolution du libellé passe par la route datalake : on observe la requête
// réellement émise, ce qui couvre aussi le choix du millésime (`targetMillesime`).
const respondWith = (rows: unknown[]) =>
  vi.spyOn(globalThis, "fetch").mockResolvedValue(
    new Response(JSON.stringify(rows), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    }),
  );

const territory = (id: string, l_territory: string) => ({
  id,
  territory: id.split("_")[0],
  l_territory,
  type: id.split("_")[1],
  year: latestMillesime,
});

const lastUrl = (spy: ReturnType<typeof respondWith>) =>
  spy.mock.calls.at(-1)![0] as string;

beforeEach(() => respondWith([]));
afterEach(() => vi.restoreAllMocks());

describe("params par défaut", () => {
  test("part de la France entière", () => {
    const { result } = renderHook(() => useDashboard());

    expect(result.current.params).toMatchObject({
      code: "XXXXX",
      name: "France",
      type: "country",
      observe: "com",
    });
  });

  test("déduit le trimestre et le semestre du mois", () => {
    const { result } = renderHook(() => useDashboard());
    const { month, trimester, semester } = result.current.params;

    expect(trimester).toBe(Math.ceil(month / 3));
    expect(semester).toBe(Math.ceil(month / 6));
  });
});

describe("onLoadTerritory", () => {
  test("résout le libellé du territoire demandé", async () => {
    const fetchSpy = respondWith([territory("69123_com", "Lyon")]);
    const { result } = renderHook(() => useDashboard());

    await act(() => result.current.onLoadTerritory({ code: "69123", type: "com" }));

    expect(lastUrl(fetchSpy)).toContain("q=69123_com");
    expect(result.current.params).toMatchObject({
      code: "69123",
      type: "com",
      name: "Lyon",
    });
  });

  test("retombe sur la France entière sans territoire", async () => {
    const { result } = renderHook(() => useDashboard());

    await act(() => result.current.onLoadTerritory());

    expect(result.current.params).toMatchObject({ code: "XXXXX", type: "country" });
  });

  // `XXXXX` est exclu du référentiel de recherche : la requête ne pourrait pas
  // aboutir, et elle partait à chaque arrivée sans `?code=`.
  test("n'interroge pas l'API pour la France entière", async () => {
    const fetchSpy = respondWith([]);
    const { result } = renderHook(() => useDashboard());

    await act(() => result.current.onLoadTerritory());

    expect(result.current.params.name).toBe("France");
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  test("garde le code brut quand la recherche floue renvoie un autre territoire", async () => {
    respondWith([territory("77139_com", "Courtry")]);
    const { result } = renderHook(() => useDashboard());

    await act(() => result.current.onLoadTerritory({ code: "69123", type: "com" }));

    expect(result.current.params.name).toBe("69123");
  });

  test("réinitialise la maille observée", async () => {
    const { result } = renderHook(() => useDashboard());
    act(() => result.current.onChangeObserve("dep"));

    await act(() => result.current.onLoadTerritory({ code: "69123", type: "com" }));

    expect(result.current.params.observe).toBe("com");
  });

  test("relâche le chargement une fois le libellé résolu", async () => {
    const { result } = renderHook(() => useDashboard());

    await act(() => result.current.onLoadTerritory({ code: "69123", type: "com" }));

    await waitFor(() => expect(result.current.loading).toBe(false));
  });

  // Régression : le millésime était figé au premier rendu (`useCallback` sans
  // dépendance), la résolution repartait sur l'année initiale après un changement.
  test("suit le millésime du tableau de bord après changement d'année", async () => {
    const fetchSpy = respondWith([territory("69123_com", "Lyon")]);
    const { result } = renderHook(() => useDashboard());

    act(() => result.current.onChangeYear(2022));
    await act(() => result.current.onLoadTerritory({ code: "69123", type: "com" }));

    expect(lastUrl(fetchSpy)).toContain("year=2022");
  });

  test("laisse l'API servir le dernier millésime sur l'année courante", async () => {
    const fetchSpy = respondWith([territory("69123_com", "Lyon")]);
    const { result } = renderHook(() => useDashboard());

    act(() => result.current.onChangeYear(latestMillesime));
    await act(() => result.current.onLoadTerritory({ code: "69123", type: "com" }));

    expect(lastUrl(fetchSpy)).not.toContain("year=");
  });
});

describe("onChangeTerritory", () => {
  test("applique le territoire choisi sans requête", () => {
    const fetchSpy = respondWith([]);
    const { result } = renderHook(() => useDashboard());

    act(() =>
      result.current.onChangeTerritory({
        territory: "244400404",
        l_territory: "Nantes Métropole",
        type: "epci",
      }),
    );

    expect(result.current.params).toMatchObject({
      code: "244400404",
      name: "Nantes Métropole",
      type: "epci",
      observe: "com",
    });
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  test("revient à la France entière sur une sélection vide", () => {
    const { result } = renderHook(() => useDashboard());
    act(() =>
      result.current.onChangeTerritory({
        territory: "69123",
        l_territory: "Lyon",
        type: "com",
      }),
    );

    act(() => result.current.onChangeTerritory(null as never));

    expect(result.current.params).toMatchObject({
      code: "XXXXX",
      name: "France",
      type: "country",
    });
  });
});

describe("setters de période et d'affichage", () => {
  test.each([
    ["onChangePeriod", "period", "year"],
    ["onChangeMonth", "month", 3],
    ["onChangeTrimester", "trimester", 2],
    ["onChangeSemester", "semester", 2],
    ["onChangeYear", "year", 2023],
    ["onChangeObserve", "observe", "epci"],
    ["onChangeGraph", "graph", 2],
    ["onChangeMap", "map", 3],
  ] as const)("%s met à jour `%s`", (setter, key, value) => {
    const { result } = renderHook(() => useDashboard());

    act(() => (result.current[setter] as (v: typeof value) => void)(value));

    expect(result.current.params[key]).toBe(value);
  });

  test("getParams fusionne sans écraser le reste", () => {
    const { result } = renderHook(() => useDashboard());
    const { month } = result.current.params;

    act(() => result.current.getParams({ year: 2024 } as never));

    expect(result.current.params).toMatchObject({ year: 2024, month, code: "XXXXX" });
  });
});
