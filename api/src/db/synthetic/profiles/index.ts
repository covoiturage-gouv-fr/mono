import { SyntheticProfile } from "../types.ts";
import { pentest } from "./pentest.ts";

export const profiles: Record<string, SyntheticProfile> = { pentest };

export function getProfile(name: string | undefined): SyntheticProfile {
  const profile = name ? profiles[name] : undefined;
  if (!profile) {
    throw new Error(`Profil inconnu '${name ?? ""}' (disponibles : ${Object.keys(profiles).join(", ")})`);
  }
  return profile;
}
