export function sessionSecretGenerator(): string {
  return crypto.randomUUID();
}
