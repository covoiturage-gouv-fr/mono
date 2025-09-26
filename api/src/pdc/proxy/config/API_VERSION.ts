/**
 * RPC API Version Configuration
 * @see https://tech.covoiturage.beta.gouv.fr/changes for a CHANGELOG
 * @documentation https://tech.covoiturage.beta.gouv.fr/
 */
export const versions = ["3.2.0", "3.3.0"];

export function lastApiVersion(): string {
  return versions[versions.length - 1];
}
