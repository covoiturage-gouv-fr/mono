export const shorten = (str: string, maxLen: number, separator = " ", end = "...") => {
  if (str.length <= maxLen) return str;
  return `${str.substring(0, str.lastIndexOf(separator, maxLen))} ${end}`;
};

// Retire la syntaxe markdown inline pour obtenir du texte brut.
const stripMarkdown = (str: string) =>
  str
    .replace(/!\[[^\]]*\]\([^)]*\)/g, "") // images
    .replace(/\[([^\]]*)\]\([^)]*\)/g, "$1") // liens -> texte
    .replace(/[*_`]+/g, "") // gras / italique / code
    .replace(/^\s*[-*+]\s+/, "") // puce de liste en tête
    .trim();

// Ligne de titre à ignorer : heading markdown ou ligne entièrement en gras/italique.
const isTitleLine = (line: string) => /^#{1,6}\s/.test(line) || (/^[*_]/.test(line) && /[*_]$/.test(line));

// Extrait un aperçu texte : ignore le(s) titre(s), garde la 1re vraie ligne, tronque.
export const excerpt = (content: string, maxLen = 200) => {
  const lines = content.split(/\r?\n/).map((l) => l.trim()).filter(Boolean);
  let i = 0;
  while (i < lines.length - 1 && isTitleLine(lines[i])) i++;
  return shorten(stripMarkdown(lines[i] ?? ""), maxLen);
};
