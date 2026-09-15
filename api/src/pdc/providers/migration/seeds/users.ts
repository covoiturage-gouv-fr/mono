import { CreateTerritoryGroupInterface, idfm, lyon } from "./territories.ts";
import { maxiCovoit, Operator } from "./operators.ts";

export const defaultStatus = "active";
export const defaultPassword =
  "$2a$10$iSm6l7.Yb9n.peL2Sgf8PumUujREjnwfCjL6orAcGN0Iowv4fqPeO";
export const defaultRole = "admin";

export interface User {
  email: string;
  firstname: string;
  lastname: string;
  status: string;
  role: string;
  password: string;
  territory?: CreateTerritoryGroupInterface;
  // Périmètres additionnels (non défaut) : fixture multi-territoire.
  extraTerritories?: CreateTerritoryGroupInterface[];
  operator?: Operator;
}

function makeUser(email: string, role: string, user: Partial<User> = {}): User {
  const name =
    (user.territory
      ? user.territory.name
      : user.operator
      ? user.operator.name
      : "registry")
      .replace(" ", "")
      .toLowerCase();
  return {
    email,
    role,
    firstname: "User",
    lastname: name,
    status: defaultStatus,
    password: defaultPassword,
    ...user,
  };
}

export const adminRegistry = makeUser("admin@example.com", "registry.admin");
export const adminIleDeFrance = makeUser(
  "territory@example.com",
  "territory.admin",
  { territory: idfm },
);
export const demoIleDeFrance = makeUser("demo@example.com", "territory.demo", {
  territory: idfm,
});
export const adminMaxiCovoit = makeUser(
  "operator@example.com",
  "operator.admin",
  { operator: maxiCovoit },
);
export const userMaxiCovoit = makeUser(
  "operator-user@example.com",
  "operator.user",
  { operator: maxiCovoit },
);
export const userRegistry = makeUser("user@example.com", "registry.user");
export const multiTerritory = makeUser(
  "multi-territory@example.com",
  "territory.admin",
  { territory: idfm, extraTerritories: [lyon] },
);
export const userTerritory = makeUser(
  "territory-user@example.com",
  "territory.user",
  { territory: idfm },
);

export const users: User[] = [
  adminRegistry,
  adminIleDeFrance,
  demoIleDeFrance,
  adminMaxiCovoit,
  userMaxiCovoit,
  userTerritory,
  userRegistry,
  multiTerritory,
];
