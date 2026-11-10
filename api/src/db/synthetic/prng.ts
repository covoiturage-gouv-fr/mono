/**
 * Générateur pseudo-aléatoire déterministe (xmur3 + mulberry32) : même graine, même suite.
 */
export class Prng {
  #state: number;

  constructor(seed: string) {
    let h = 1779033703 ^ seed.length;
    for (let i = 0; i < seed.length; i++) {
      h = Math.imul(h ^ seed.charCodeAt(i), 3432918353);
      h = (h << 13) | (h >>> 19);
    }
    h = Math.imul(h ^ (h >>> 16), 2246822507);
    h = Math.imul(h ^ (h >>> 13), 3266489909);
    this.#state = (h ^ (h >>> 16)) >>> 0;
  }

  /** Flottant dans [0, 1[ */
  next(): number {
    this.#state = (this.#state + 0x6d2b79f5) >>> 0;
    let t = this.#state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  }

  /** Entier dans [min, max], bornes incluses */
  int(min: number, max: number): number {
    return min + Math.floor(this.next() * (max - min + 1));
  }

  pick<T>(list: readonly T[]): T {
    if (!list.length) throw new Error("Tirage dans une liste vide");
    return list[this.int(0, list.length - 1)];
  }

  /** Tirage pondéré : les poids n'ont pas besoin de sommer à 1 */
  weighted<T extends string>(weights: Partial<Record<T, number>>): T {
    const entries = Object.entries(weights) as [T, number][];
    const total = entries.reduce((sum, [, w]) => sum + w, 0);
    if (total <= 0) throw new Error("Tirage pondéré sans poids positif");
    let cursor = this.next() * total;
    for (const [key, weight] of entries) {
      cursor -= weight;
      if (cursor < 0) return key;
    }
    return entries[entries.length - 1][0];
  }

  hex(length: number): string {
    let out = "";
    while (out.length < length) out += this.int(0, 15).toString(16);
    return out;
  }

  /** UUID v4 bien formé, mais déterministe */
  uuid(): string {
    const h = this.hex(32);
    const variant = "89ab"[this.int(0, 3)];
    return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-${variant}${h.slice(17, 20)}-${h.slice(20)}`;
  }
}
