// eslint-disable-next-line @typescript-eslint/prefer-nullish-coalescing -- variable vide (cf. .env.example) doit retomber sur la valeur par défaut
const apiUrl = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8080";

/**
 * Best-effort hit counter for generated certificates.
 * Fire-and-forget: any failure (network, CORS, server) is swallowed so it can
 * never break the attestation download. Server-side CORS + rate limiting are
 * the real gates (see api HttpTransport.ts POST /honor).
 */
export async function saveHonor(type: "public" | "limited", employer: string): Promise<void> {
  try {
    await fetch(`${apiUrl}/honor`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ type, employer }),
    });
  } catch {
    // ignore
  }
}
