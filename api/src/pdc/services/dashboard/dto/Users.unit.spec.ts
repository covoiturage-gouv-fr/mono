import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { validate } from "@/lib/superstruct/index.ts";
import { CreateUser, UpdateUser } from "./Users.ts";

// Contrat du formulaire utilisateur du dashboard : ce que le front sérialise doit passer.
describe("dto/Users", () => {
  const failures = (payload: unknown, struct: typeof UpdateUser | typeof CreateUser): string[] => {
    const [err] = validate(payload, struct, { coerce: true });
    return err ? err.failures().map((f) => `${f.path.join(".")} : ${f.message}`) : [];
  };

  const base = {
    firstname: "Jean",
    lastname: "Dupont",
    email: "jean@example.com",
    role: "territory.admin",
    operator_id: null,
    territory_id: 7,
  };

  it("accepte les périmètres au format {territory_id, is_default}", () => {
    const scopes = [{ territory_id: 7, is_default: true }, { territory_id: 9, is_default: false }];
    assertEquals(failures({ ...base, scopes }, CreateUser), []);
    assertEquals(failures({ ...base, id: 42, scopes }, UpdateUser), []);
  });

  // scopes_count est une sortie de la liste, pas une entrée : le front ne doit pas la réémettre.
  it("refuse scopes_count, champ de sortie uniquement", () => {
    assertEquals(failures({ ...base, id: 42, scopes_count: 2 }, UpdateUser).length > 0, true);
  });

  it("refuse un périmètre sans territory_id", () => {
    assertEquals(failures({ ...base, scopes: [{ is_default: true }] }, CreateUser).length > 0, true);
  });

  it("refuse un login_siren qui n'a pas 9 chiffres", () => {
    assertEquals(failures({ ...base, login_siren: "1234" }, CreateUser).length > 0, true);
  });
});
