export function certFilename(name: string): string {
  const cleanName = name
    .trim()
    .replace(/[^A-ZÀ-ÖØ-öø-ÿ]/gi, '')
    .replace(/\s/g, '-')
    .toLowerCase();

  return `attestation-${cleanName}-${new Date().getTime()}.pdf`;
}
