// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, test, vi } from "vitest";
import SearchSelect, { normalize } from "./SearchSelect";

const options = [
  { id: 1, label: "Métropole de Lyon" },
  { id: 2, label: "Île-de-France Mobilités" },
  { id: 3, label: "Karos" },
];

afterEach(cleanup);

const setup = (value: number | null = null) => {
  const onChange = vi.fn();
  render(
    <SearchSelect
      options={options}
      value={value}
      onChange={onChange}
      label="Territoire"
      noOptionsText="Aucun"
    />,
  );
  const input = screen.getByRole("combobox");
  fireEvent.focus(input);
  return { input, onChange };
};

const shown = () => screen.queryAllByRole("option").map((o) => o.textContent);

describe("filtre", () => {
  test("liste tout sans saisie", () => {
    const { input } = setup();
    fireEvent.mouseDown(input);
    expect(shown()).toEqual(options.map((o) => o.label));
  });

  test("ignore casse et accents", () => {
    const { input } = setup();
    fireEvent.change(input, { target: { value: "ILE DE france" } });
    expect(shown()).toEqual(["Île-de-France Mobilités"]);
  });

  test("affiche le message quand rien ne correspond", () => {
    const { input } = setup();
    fireEvent.change(input, { target: { value: "zzz" } });
    expect(screen.getByText("Aucun")).toBeTruthy();
  });
});

describe("sélection", () => {
  test("renvoie id et libellé", () => {
    const { input, onChange } = setup();
    fireEvent.change(input, { target: { value: "karos" } });
    fireEvent.click(screen.getByRole("option"));
    expect(onChange).toHaveBeenCalledWith(3, "Karos");
  });

  test("préremplit la valeur courante", () => {
    const { input } = setup(1);
    expect((input as HTMLInputElement).value).toBe("Métropole de Lyon");
  });
});

describe("normalize", () => {
  test("neutralise casse, accents, tirets et espaces multiples", () => {
    expect(normalize("  Île-de-France   MOBILITÉS ")).toBe(
      "ile de france mobilites",
    );
  });
});
