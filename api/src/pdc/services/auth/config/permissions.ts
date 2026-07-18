const permissions = {
  // Journeys acquisition
  "acquisition.create": ["operator.application", "operator.admin"],
  "acquisition.cancel": ["operator.application", "operator.admin"],
  "acquisition.status": [
    "operator.user",
    "operator.application",
    "operator.admin",
  ],

  // DEX credentials and access tokens
  "credentials.create": ["registry.admin", "operator.admin"],
  "credentials.read": ["registry.admin", "operator.admin"],
  "credentials.delete": ["registry.admin", "operator.admin"],
  "access_token.create": ["operator.application"],

  // Companies
  "company.fetch": ["common"],
  "company.find": ["common"],

  // Honor certificates
  "honor.save": ["common"],
  "honor.stats": ["common"],

  "observatory.stats": ["common"],

  // Operators CRUD
  "operator.create": ["registry.admin"],
  "operator.list": ["common"],
  "operator.find": [
    "common",
    "operator.user",
    "operator.admin",
    "registry.admin",
  ],
  "operator.delete": ["registry.admin"],
  "operator.update": ["operator.admin", "registry.admin"],

  // @deprecated
  "operator.patchContacts": ["operator.admin", "registry.admin"],

  // @deprecated
  "operator.patchThumbnail": ["operator.admin", "registry.admin"],

  // Campaigns
  "policy.create": ["territory.admin", "registry.admin"],

  "policy.find": [
    "territory.user",
    "territory.admin",
    "registry.user",
    "registry.admin",
    "operator.admin",
    "operator.user",
  ],

  "policy.list": [
    "common",
    "territory.user",
    "territory.admin",
    "registry.user",
    "registry.admin",
  ],

  // @deprecated
  "policy.patch": ["territory.admin", "registry.admin"],
  "policy.list.templates": ["common"],

  // Territories
  "territory.create": ["registry.admin"],

  // @deprecated
  "territory.delete": ["registry.admin"],
  "territory.find": [
    "common",
    "territory.user",
    "territory.admin",
    "registry.user",
    "registry.admin",
  ],

  // @deprecated
  "territory.update": ["territory.admin", "registry.admin"],
  "territory.list": ["common"],
  "territory.read": [
    "common",
    "territory.user",
    "territory.admin",
    "registry.user",
    "registry.admin",
  ],

  // @deprecated
  "territory.patchContacts": ["territory.admin", "registry.admin"],

  // APDF
  "apdf.list": [
    "operator.admin",
    "territory.admin",
    "registry.admin",
    "operator.user",
  ],
  "apdf.listCurrentMonth": ["registry.admin"],
  "apdf.export": ["registry.admin"],

  // TODO Check if still in use
  // Users
  "user.update": [
    "common",
    "operator.admin",
    "territory.admin",
    "registry.admin",
  ],
  "user.create": ["operator.admin", "territory.admin", "registry.admin"],
  "user.delete": ["operator.admin", "territory.admin", "registry.admin"],
  "user.find": [
    "common",
    "operator.admin",
    "territory.admin",
    "registry.admin",
  ],
  "user.list": [
    "operator.user",
    "operator.admin",
    "territory.user",
    "territory.admin",
    "registry.user",
    "registry.admin",
  ],
  "user.sendEmail": ["operator.admin", "territory.admin", "registry.admin"],

  // Champs/opérations privilégiés (login_siren, octroi de scope multi-territoire) : registry.admin seul.
  "user.manageScopes": ["registry.admin"],

  // export service
  "export.create": ["common"],
  "export.list": ["common"],
  "export.read": ["common"],
  "export.status": ["common"],
  "export.cancel": ["common"],
  "export.download": ["common"],

  // datalake export worker (control plane: claim/complete/fail)
  "export.process": ["datalake.worker"],
};

function scopeToGroup(permissionName: string, group: string) {
  return `${group}.${permissionName}`;
}

function dispatchPermissionsFromMatrix(
  permissionsObject: Record<string, string[]>,
) {
  const permissionsByGroup: Record<string, string[]> = {
    common: [],
    "territory.user": [],
    "territory.admin": [],
    "operator.user": [],
    "operator.admin": [],
    "operator.application": [],
    "registry.user": [],
    "registry.admin": [],
    "datalake.worker": [],
  };

  for (const permissionName of Reflect.ownKeys(permissionsObject) as string[]) {
    const permissionRoles = permissionsObject[permissionName];

    for (const permissionRole of permissionRoles) {
      const group = permissionRole.split(".")[0];
      permissionsByGroup[permissionRole].push(
        scopeToGroup(permissionName, group),
      );
    }
  }
  // prefix common
  return permissionsByGroup;
}

const permissionsByRoles = dispatchPermissionsFromMatrix(permissions);

// Machine/system roles do NOT inherit the `common` permission set granted to
// every human user — they hold only their explicitly-scoped permissions
// (least privilege). e.g. the datalake export worker holds only export.process.
const SYSTEM_ROLES: ReadonlySet<string> = new Set(["datalake.worker"]);

export function getPermissions(role: string): string[] {
  if (SYSTEM_ROLES.has(role)) {
    return [...(permissionsByRoles[role] ?? [])];
  }
  return [
    ...permissionsByRoles["common"],
    ...permissionsByRoles[role],
  ];
}

export const territory = {
  admin: {
    slug: "admin",
    name: "Admin",
    permissions: [
      ...permissionsByRoles["common"],
      ...permissionsByRoles["territory.admin"],
    ],
  },
  user: {
    slug: "user",
    name: "User",
    permissions: [
      ...permissionsByRoles["common"],
      ...permissionsByRoles["territory.user"],
    ],
  },
};

export const operator = {
  admin: {
    slug: "admin",
    name: "Admin",
    permissions: [
      ...permissionsByRoles["common"],
      ...permissionsByRoles["operator.admin"],
    ],
  },
  user: {
    slug: "user",
    name: "User",
    permissions: [
      ...permissionsByRoles["common"],
      ...permissionsByRoles["operator.user"],
    ],
  },
};

export const registry = {
  admin: {
    slug: "admin",
    name: "Admin",
    permissions: [
      ...permissionsByRoles["common"],
      ...permissionsByRoles["registry.admin"],
    ],
  },
  user: {
    slug: "user",
    name: "User",
    permissions: [
      ...permissionsByRoles["common"],
      ...permissionsByRoles["registry.user"],
    ],
  },
};
