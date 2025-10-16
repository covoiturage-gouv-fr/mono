const permissions = {
  "acquisition.create": ["operator.application", "operator.admin"],
  "acquisition.cancel": ["operator.application", "operator.admin"],
  "acquisition.status": [
    "operator.user",
    "operator.application",
    "operator.admin",
  ],
  "application.create": ["operator.admin"],
  "application.find": ["operator.user", "operator.admin"],
  "application.list": ["operator.user", "operator.admin"],
  "application.revoke": ["operator.admin"],
  "credentials.create": ["registry.admin", "operator.admin"],
  "credentials.read": ["registry.admin", "operator.admin"],
  "credentials.delete": ["registry.admin", "operator.admin"],
  "access_token.create": ["operator.application"],
  "company.fetch": ["common"],
  "company.find": ["common"],
  "honor.save": ["common"],
  "honor.stats": ["common"],
  "monitoring.journeysstats": ["registry.admin"],
  "observatory.stats": ["common"],
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
  "operator.patchContacts": ["operator.admin", "registry.admin"],
  "operator.patchThumbnail": ["operator.admin", "registry.admin"],
  "policy.create": ["territory.demo", "territory.admin", "registry.admin"],
  "policy.delete": ["territory.demo", "territory.admin", "registry.admin"],
  "policy.find": [
    "territory.demo",
    "territory.user",
    "territory.admin",
    "registry.user",
    "registry.admin",
    "operator.admin",
    "operator.user",
  ],
  "policy.launch": ["territory.admin"],
  "policy.list": [
    "common",
    "territory.demo",
    "territory.user",
    "territory.admin",
    "registry.user",
    "registry.admin",
  ],
  "policy.patch": ["territory.admin", "registry.admin"],
  "policy.simulate.past": [
    "territory.admin",
    "territory.demo",
    "registry.admin",
  ],
  "policy.simulate.fake": ["territory.admin", "registry.admin"],
  "policy.simulate.future": [
    "operator.application",
    "operator.admin",
    "territory.admin",
  ],
  "policy.list.templates": ["common"],
  "territory.create": ["registry.admin"],
  "territory.delete": ["registry.admin"],
  "territory.find": [
    "common",
    "territory.demo",
    "territory.user",
    "territory.admin",
    "registry.user",
    "registry.admin",
  ],
  "territory.update": ["territory.admin", "registry.admin"],
  "territory.list": ["common"],
  "territory.read": [
    "common",
    "territory.user",
    "territory.admin",
    "registry.user",
    "registry.admin",
  ],
  "territory.patchOperator": ["operator.admin"],
  "territory.patchContacts": ["territory.admin", "registry.admin"],
  "trip.stats": [
    "common",
    "operator.user",
    "operator.admin",
    "territory.demo",
    "territory.user",
    "territory.admin",
    "registry.user",
    "registry.admin",
  ],
  "trip.export": [
    "operator.user",
    "operator.admin",
    "territory.user",
    "territory.admin",
    "registry.user",
    "registry.admin",
  ],
  "apdf.list": [
    "operator.admin",
    "territory.admin",
    "registry.admin",
    "operator.user",
  ],
  "apdf.listCurrentMonth": ["registry.admin"],
  "apdf.export": ["registry.admin"],
  "trip.list": [
    "operator.user",
    "operator.admin",
    "territory.demo",
    "territory.user",
    "territory.admin",
    "registry.user",
    "registry.admin",
  ],
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
    "territory.demo",
    "territory.user",
    "territory.admin",
    "registry.user",
    "registry.admin",
  ],
  "user.sendEmail": ["operator.admin", "territory.admin", "registry.admin"],
  "user.policySimulate": ["common"],

  // export service
  "export.create": ["common"],
  "export.list": ["common"],
  "export.read": ["common"],
  "export.status": ["common"],
  "export.cancel": ["common"],
  "export.download": ["common"],
};

function scopeToGroup(permissionName: string, group: string) {
  return `${group}.${permissionName}`;
}

function dispatchPermissionsFromMatrix(
  permissionsObject: Record<string, string[]>,
) {
  const permissionsByGroup: Record<string, string[]> = {
    common: [],
    "territory.demo": [],
    "territory.user": [],
    "territory.admin": [],
    "operator.user": [],
    "operator.application": [],
    "operator.admin": [],
    "registry.user": [],
    "registry.admin": [],
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

export const territory = {
  admin: {
    slug: "admin",
    name: "Admin",
    permissions: [
      ...permissionsByRoles["common"],
      ...permissionsByRoles["territory.admin"],
    ],
  },
  demo: {
    slug: "demo",
    name: "Demo",
    permissions: [
      ...permissionsByRoles["common"],
      ...permissionsByRoles["territory.demo"],
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
  application: {
    slug: "application",
    name: "Application",
    permissions: [
      ...permissionsByRoles["common"],
      ...permissionsByRoles["operator.application"],
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
