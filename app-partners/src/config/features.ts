// Inliné au build (export statique) : activer avec NEXT_PUBLIC_FEATURE_MULTI_SIRET=true, NEXT_PUBLIC_FEATURE_EXTERNAL_CAMPAIGNS=true.
export const features = {
  externalCampaigns: process.env.NEXT_PUBLIC_FEATURE_EXTERNAL_CAMPAIGNS === "true",
  multiSiret: process.env.NEXT_PUBLIC_FEATURE_MULTI_SIRET === "true",
};
