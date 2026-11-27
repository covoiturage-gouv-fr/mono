// @vitest-environment jsdom
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, test, vi } from "vitest";
import UserScopesEditor from "./UserScopesEditor";

afterEach(cleanup);

describe("UserScopesEditor", () => {
  test("place le champ d'ajout au-dessus du tableau des périmètres", () => {
    render(
      <UserScopesEditor scopes={[{ territory_id: 1, is_default: true, territory_name: "Lyon" }]} onChange={vi.fn()} />,
    );

    const add = screen.getByRole("combobox");
    const table = screen.getByRole("table");
    expect(add.compareDocumentPosition(table) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });
});
