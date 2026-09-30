/**
 * 1. Import the configuration file
 * 2. add the configuration to the object in objectToMap()
 */
import { observatoire } from './observatoire';
import { analytics } from './analytics';
import { cms } from './cms';
import { search } from './search';

const _configuration = objectToMap({
  cms,
  observatoire,
  analytics,
  search,
  next: {
    public_datalake_base_url: process.env.NEXT_PUBLIC_DATALAKE_BASE_URL,
    public_url: process.env.NEXT_PUBLIC_URL,
  },
});

// ---------------------------------------------------------------------------------------
// Helpers and export
// ---------------------------------------------------------------------------------------

export type ConfigObject = string | number | boolean | null | { [key: string]: ConfigObject } | undefined;

function objectToMap(obj: ConfigObject): Map<string, ConfigObject> {
  const map = new Map();
  if (typeof obj === 'object' && obj !== null) {
    for (const key in obj) {
      if (obj.hasOwnProperty(key)) {
        const value = obj[key];

        if (typeof value === 'object' && value !== null) {
          map.set(key, objectToMap(value)); // Recursively convert nested objects
        } else {
          map.set(key, value);
        }
      }
    }
  }
  return map;
}

export const Config = {
  get<T>(key: string, defaultValue?: T): T {
    let _value: unknown = _configuration;
    for (const part of key.split('.')) {
      _value = _value instanceof Map && _value.has(part) ? _value.get(part) : undefined;
    }
    if (typeof _value === 'undefined') {
      if (typeof defaultValue === 'undefined') {
        throw new Error(`Configuration key "${key}" not found`);
      }
      return defaultValue;
    }
    return _value as T;
  },
};
