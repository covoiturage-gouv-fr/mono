export interface Violation {
  path: string[];
  message: string;
}

// L'API sérialise chaque violation superstruct en « chemin : message » (ValidatorMiddleware) ; le chemin est vide pour une règle croisée.
const VIOLATION = /^([^:]*) : ([\s\S]+)$/;

export const parseViolations = (body: unknown): Violation[] | undefined => {
  if (!Array.isArray(body) || body.length === 0) return undefined;
  const parsed = body.map((line) => (typeof line === "string" ? VIOLATION.exec(line) : null));
  if (parsed.some((m) => m === null)) return undefined;
  return parsed.map((m) => ({ path: m![1] ? m![1].split(",") : [], message: m![2] }));
};

const MAX_INT_BOUND = 2147483647;

// Phrase lisible pour un message superstruct ; un message déjà en français est relayé.
export const violationReason = (message: string): string => {
  if (/received: (null|undefined)$/.test(message)) return "valeur requise";
  if (message.startsWith("Expected an integer")) return "nombre entier attendu";
  const range = /^Expected an? integer between `(\d+)` and `(\d+)`/.exec(message);
  if (range) {
    return Number(range[2]) >= MAX_INT_BOUND
      ? "nombre positif attendu"
      : `nombre entre ${range[1]} et ${range[2]} attendu`;
  }
  if (/^Expected an? array with a length between .* length of `0`/.test(message)) return "au moins une valeur requise";
  if (message.startsWith("Expected a string matching `/^\\d{4}-")) return "date attendue (AAAA-MM-JJ)";
  if (message.startsWith("Expected a string matching `/^https")) return "adresse web attendue (https://…)";
  if (message.startsWith("Expected a string matching")) return "format invalide";
  if (message.startsWith("Expected one of")) return "valeur non reconnue";
  if (message.startsWith("Expected a value of type `boolean`")) return "oui ou non attendu";
  if (/^Expected an? string/.test(message)) return "texte attendu";
  if (message.startsWith("Expected ")) return "valeur invalide";
  return message.charAt(0).toLowerCase() + message.slice(1);
};
