/**
 * 1. Import the configuration file
 * 2. add the configuration to the object in objectToMap()
 */
import { analytics } from "./analytics";
import { auth } from "./auth";
import { search } from "./search";

const objectToMap = (obj: ConfigObject): Map<string, ConfigObject> => {
  const map = new Map<string, ConfigObject>();
  if (typeof obj === "object" && obj !== null) {
    for (const key in obj) {
      if (key in obj) {
        const value = (obj as Record<string, ConfigObject>)[key];

        if (typeof value === "object" && value !== null) {
          map.set(key, objectToMap(value)); // Recursively convert nested objects
        } else {
          map.set(key, value);
        }
      }
    }
  }
  return map;
};

const nextEnvironmentVariables = (): ConfigObject => {
  return Object.entries(process.env)
    .filter(([key]) => key.startsWith("NEXT_"))
    .filter(([key]) => typeof process.env[key] !== "undefined")
    .reduce(
      (acc, [key]) => {
        const k = key.replace("NEXT_", "").toLowerCase();
        acc[k] = process.env[key];
        return acc;
      },
      {} as Record<string, ConfigObject>,
    );
};

const _configuration = objectToMap({
  analytics,
  search,
  auth,
  next: nextEnvironmentVariables(),
});

// ---------------------------------------------------------------------------------------
// Helpers and export
// ---------------------------------------------------------------------------------------

export type ConfigObject =
  | string
  | number
  | boolean
  | null
  | { [key: string]: ConfigObject }
  | undefined
  | Map<string, ConfigObject>;

export const Config = {
  get<T>(key: string, defaultValue?: T): T {
    let _value: unknown = _configuration;
    for (const part of key.split(".")) {
      _value = _value instanceof Map && _value.has(part)
        ? _value.get(part)
        : undefined;
    }
    if (typeof _value === "undefined") {
      if (typeof defaultValue === "undefined") {
        throw new Error(`Configuration key "${key}" not found`);
      }
      return defaultValue;
    }
    return _value as T;
  },
};
