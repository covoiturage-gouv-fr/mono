// Inliné au build (export statique) : activer avec NEXT_PUBLIC_FEATURE_MULTI_SIRET=true.
export const features = {
  multiSiret: process.env.NEXT_PUBLIC_FEATURE_MULTI_SIRET === "true",
};
