function luhnSum(digits: string): number {
  let sum = 0;
  for (let i = 0; i < digits.length; i++) {
    let d = Number(digits[digits.length - 1 - i]);
    if (i % 2 === 1) {
      d *= 2;
      if (d > 9) d -= 9;
    }
    sum += d;
  }
  return sum;
}

export function isLuhnValid(digits: string): boolean {
  return /^[0-9]+$/.test(digits) && luhnSum(digits) % 10 === 0;
}

/** Chiffre de contrôle à ajouter à `base` pour obtenir un numéro valide */
export function luhnKey(base: string): string {
  if (!/^[0-9]+$/.test(base)) throw new Error(`Base non numérique : '${base}'`);
  return String((10 - (luhnSum(`${base}0`) % 10)) % 10);
}

/** Un SIRET valide a un SIREN (9 premiers chiffres) lui-même valide */
export function isValidSiret(siret: string): boolean {
  return siret.length === 14 && isLuhnValid(siret) && isLuhnValid(siret.slice(0, 9));
}
